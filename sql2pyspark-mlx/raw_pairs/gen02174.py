from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "service", "host", F.explode_outer("context").alias("attr_key", "v"))
)

result = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "attr_key",
        "v",
        F.when(F.col("attr_key").rlike("^(trace|region)$"), "PRIORITY").otherwise("GROUP_B").alias("attr_key_zone"),
    )
)
