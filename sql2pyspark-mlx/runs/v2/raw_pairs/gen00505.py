from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "segment")
    .agg(F.min("age").alias("min_age"))
    .orderBy("city", F.col("min_age").desc())
)
