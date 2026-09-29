from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .select(
        "host",
        "latency_ms",
        "attr_key",
        "v",
        F.when(F.col("host").rlike("^(h4|h2)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("host_zone"),
    )
    .orderBy("latency_ms")
)
