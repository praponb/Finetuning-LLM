from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    exploded
    .select(
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "k",
        "v",
        F.when(F.col("host").rlike("^(h4|h1)$"), "CORE").when(F.col("host").rlike("^(h2|h3)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("host_zone"),
    )
    .orderBy(F.col("log_ts").desc(), "latency_ms")
)
