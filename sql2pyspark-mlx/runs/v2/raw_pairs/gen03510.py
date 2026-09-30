from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", "country")
    .agg(F.count("credit_limit").alias("credit_limit_count"))
    .filter(F.col("credit_limit_count") > 4)
)
