from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "host",
        "level",
        "k",
        F.when(F.col("host").rlike("^(h4|h3)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("host_zone"),
    )
    .orderBy(F.col("level").desc())
)
