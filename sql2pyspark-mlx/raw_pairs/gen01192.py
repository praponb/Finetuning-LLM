from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(F.count("age").alias("age_count"))
    .filter(F.col("age_count") > 10)
)
