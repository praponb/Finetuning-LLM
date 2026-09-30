from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") >= 6790) & F.col("city").isin(["Bangkok", "New York", "Chiang Mai"]))
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
)
