from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "customer_id",
        F.upper("device").alias("device_upper"),
        F.substring("country", 1, 3).alias("country_prefix"),
    )
)
