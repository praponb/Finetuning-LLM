from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        F.concat_ws("/", "browser", "device").alias("browser_device_key"),
    )
)
