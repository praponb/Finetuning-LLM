from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.max("opened_date").alias("latest_opened_date"),
        F.sum(F.when(F.col("region") == "East", 1).otherwise(0)).alias("east_count"),
    )
    .orderBy(F.col("east_count").desc(), "region")
)
