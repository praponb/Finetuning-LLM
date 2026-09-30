from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "browser",
        "customer_id",
        F.lower("device").alias("device_lower"),
        F.lower("country").alias("country_lower"),
    )
)
