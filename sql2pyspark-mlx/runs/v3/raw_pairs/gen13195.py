from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        F.trim("device").alias("clean_device"),
        F.concat_ws("-", "browser", "device").alias("browser_device_key"),
    )
    .orderBy(F.col("customer_id").desc())
)
