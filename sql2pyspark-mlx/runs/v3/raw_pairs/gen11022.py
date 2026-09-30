from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
    .filter(F.col("max_sqft") < 14100)
    .select("opened_month", "min_sqft", "max_sqft")
)
