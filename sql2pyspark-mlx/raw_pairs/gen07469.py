from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "service", "host", "log_ts", F.explode("context").alias("key", "v"))
)

result = (
    lines
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "key",
        "v",
        F.when(F.col("service").rlike("^(checkout|search)$"), "CORE").otherwise("OTHER_ZONE").alias("service_zone"),
    )
    .orderBy(F.col("key").asc_nulls_last())
)
