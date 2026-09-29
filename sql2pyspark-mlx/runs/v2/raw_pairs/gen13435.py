from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(F.avg("age").alias("avg_age"))
    .orderBy(F.col("avg_age").desc(), "is_active")
    .limit(50)
)
