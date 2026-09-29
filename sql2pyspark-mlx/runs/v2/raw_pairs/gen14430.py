from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") >= 58) & F.col("segment").isNotNull())
    .groupBy("is_active")
    .agg(F.count("age").alias("age_count"))
    .filter(F.col("age_count") >= 1)
)
