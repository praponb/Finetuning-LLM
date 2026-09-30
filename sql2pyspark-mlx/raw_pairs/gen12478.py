from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "k",
        "value",
        F.when(F.col("k").rlike("^(region|version)$"), "PRIORITY").when(F.col("k").rlike("^(trace)$"), "CORE").otherwise("GROUP_B").alias("k_zone"),
    )
)
