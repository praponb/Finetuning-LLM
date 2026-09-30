from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([4, 8, 12]))
    .groupBy("segment", "country")
    .agg(F.count("credit_limit").alias("credit_limit_count"))
    .filter(F.col("credit_limit_count") > 3)
)
