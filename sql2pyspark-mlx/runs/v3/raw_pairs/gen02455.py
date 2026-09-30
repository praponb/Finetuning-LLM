from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2025-08-03")
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("total_sqft") >= 88000)
)
