import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random

def generate_metrics(n_points=5000, anomaly_frac=0.02):
    timestamps = [datetime.now() + timedelta(seconds=i) for i in range(n_points)]

    cpu = np.sin(np.linspace(0, 20, n_points)) * 10 + 50 + np.random.normal(0, 2, n_points)
    memory = np.cos(np.linspace(0, 15, n_points)) * 5 + 60 + np.random.normal(0, 2, n_points)
    latency = np.random.normal(200, 20, n_points)

    labels = np.zeros(n_points)

    anomaly_indices = np.random.choice(
        range(n_points),
        size=int(n_points * anomaly_frac),
        replace=False
    )

    for idx in anomaly_indices:
        cpu[idx] += random.uniform(20, 40)
        latency[idx] += random.uniform(200, 400)
        labels[idx] = 1

    df = pd.DataFrame({
        "timestamp": timestamps,
        "cpu": cpu,
        "memory": memory,
        "latency": latency,
        "anomaly": labels
    })

    return df


LOG_TEMPLATES = {
    0: [
        "Service heartbeat normal",
        "Connection pool healthy",
        "Routine background job completed"
    ],
    1: [
        "Database timeout detected",
        "Service unavailable",
        "High memory pressure",
        "Request latency exceeded threshold"
    ]
}


def generate_logs(metric_df):
    logs = []

    for _, row in metric_df.iterrows():
        label = int(row["anomaly"])
        msg = random.choice(LOG_TEMPLATES[label])

        logs.append({
            "timestamp": row["timestamp"],
            "message": msg,
            "anomaly": label
        })

    return pd.DataFrame(logs)


if __name__ == "__main__":
    metrics = generate_metrics()
    logs = generate_logs(metrics)

    metrics.to_csv("data/raw/metrics.csv", index=False)
    logs.to_csv("data/raw/logs.csv", index=False)

    print("Generated metrics + logs.")

