from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.when(F.col("service").rlike("^(checkout|search)$"), "PRIORITY").otherwise("EXTENDED").alias("service_zone"),
    )
    .orderBy("service_zone")
)
