from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "log_ts", "latency_ms", F.explode_outer("context").alias("k", "v"))
)

result = (
    lines
    .select(
        "log_ts",
        "latency_ms",
        "k",
        "v",
        F.when(F.col("k").rlike("^(region|trace)$"), "EXTENDED").otherwise("CORE").alias("k_zone"),
    )
    .orderBy(F.col("k_zone").desc())
)
