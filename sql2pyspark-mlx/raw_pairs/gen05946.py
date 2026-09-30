from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "key",
        "attr_value",
        F.when(F.col("service").rlike("^(checkout|search)$"), "PRIORITY").otherwise("CORE").alias("service_zone"),
    )
)
