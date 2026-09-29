from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "key",
        "v",
        F.when(F.col("service").rlike("^(search|billing)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("service_zone"),
    )
    .orderBy(F.col("host").asc_nulls_last(), F.col("service_zone").desc())
)
