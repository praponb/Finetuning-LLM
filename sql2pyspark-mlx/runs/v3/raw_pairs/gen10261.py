from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull() & (F.col("sqft") >= 13786) & (F.col("opened_date") > "2025-03-01"))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft")).otherwise(0)).alias("singapore_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") < 80)
    .orderBy(F.col("singapore_sqft").desc())
)
