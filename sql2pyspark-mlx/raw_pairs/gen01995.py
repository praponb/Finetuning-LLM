from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "value",
        F.when(F.col("host").rlike("^(h2|h1)$"), "EXTENDED").otherwise("PRIORITY").alias("host_zone"),
    )
    .orderBy(F.col("host").desc())
)
