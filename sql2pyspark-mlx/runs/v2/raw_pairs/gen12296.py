from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.length("store_name") >= 7) & F.col("store_name").isNotNull())
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.max("opened_date").alias("last_opened_date"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
    )
    .orderBy("east_sqft")
)
