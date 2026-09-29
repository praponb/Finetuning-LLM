from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull() & (F.col("country") != "US"))
    .groupBy("city")
    .agg(F.count("age").alias("age_count"))
    .orderBy("city")
)
