from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .agg(F.round(F.avg("age"), 2).alias("avg_age"))
    .orderBy(F.col("avg_age").desc())
)
