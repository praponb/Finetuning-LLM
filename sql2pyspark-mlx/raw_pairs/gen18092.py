from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["TH", "SG", "JP"]))
    .select(
        "id",
        "device",
        F.concat_ws(",", "browser", "device").alias("browser_device_key"),
        F.when(F.col("device") == "desktop", 1).otherwise(0).alias("is_desktop"),
    )
)
