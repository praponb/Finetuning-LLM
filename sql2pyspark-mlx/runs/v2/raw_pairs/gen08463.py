from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 10013) & F.month("opened_date").isin([5, 10, 11]))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
        F.sum("sqft").alias("total_sqft"),
    )
    .orderBy("city")
)
