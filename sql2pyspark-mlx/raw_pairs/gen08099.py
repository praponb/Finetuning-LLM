from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & (F.col("credit_limit") >= 7900) & (F.col("city") == "Singapore"))
    .groupBy("country")
    .agg(F.sum("age").alias("total_age"))
    .orderBy(F.col("country").desc())
    .limit(5)
)
