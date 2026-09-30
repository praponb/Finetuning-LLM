from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNotNull())
    .groupBy("segment", "country")
    .agg(F.count("credit_limit").alias("credit_limit_count"))
    .filter(F.col("credit_limit_count") < 7)
    .select("segment", "country", "credit_limit_count")
    .orderBy("country")
)
