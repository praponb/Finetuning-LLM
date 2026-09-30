from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "device",
        F.lower("country").alias("country_lower"),
        F.when(F.col("device") == "desktop", 1).otherwise(0).alias("is_desktop"),
    )
    .orderBy("device", "id")
)
