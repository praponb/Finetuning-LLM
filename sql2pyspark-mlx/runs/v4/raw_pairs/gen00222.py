from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("city", "is_active")
    .agg(F.count("*").alias("customer_count"))
    .filter(F.col("customer_count") < 5)
    .orderBy("city")
)
