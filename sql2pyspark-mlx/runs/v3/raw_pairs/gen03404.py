from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([7, 20]) & F.col("customer_id").isNull())
    .select(
        "browser",
        "device",
        F.when(F.col("device") == "desktop", 1).otherwise(0).alias("is_desktop"),
    )
)
