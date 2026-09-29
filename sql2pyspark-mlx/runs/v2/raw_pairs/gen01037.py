from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .filter(F.col("east_sqft") < 37)
)
