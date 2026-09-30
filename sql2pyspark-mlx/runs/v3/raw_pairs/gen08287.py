from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") < 9938)
    .groupBy(
        "region",
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "city",
    )
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("sqft")).otherwise(0)).alias("chiang_mai_sqft"),
        F.sum("sqft").alias("total_sqft"),
        F.avg("sqft").alias("avg_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
    .filter(F.col("max_sqft") > 7300)
    .drop("max_sqft")
    .orderBy(F.col("avg_sqft").desc())
)
