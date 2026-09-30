from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "host",
        "k",
        F.when(F.col("k").rlike("^(version|trace)$"), "GROUP_B").otherwise("EXTENDED").alias("k_zone"),
    )
    .orderBy(F.col("host").desc())
)
