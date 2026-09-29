from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.count("age").alias("age_count"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
    .filter(F.col("age_count") >= 7)
    .select("city", "age_count", "credit_limit_range")
    .orderBy("city")
    .limit(20)
)
