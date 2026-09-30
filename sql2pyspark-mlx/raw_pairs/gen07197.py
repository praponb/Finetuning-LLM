from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "host",
        "level",
        "key",
        "value",
        F.when(F.col("host").rlike("^(h2|h4)$"), "PRIORITY").when(F.col("host").rlike("^(h1|h3)$"), "OTHER_ZONE").otherwise("CORE").alias("host_zone"),
    )
)
