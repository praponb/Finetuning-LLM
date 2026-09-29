from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@outlook.com"))
    .groupBy("country", "segment")
    .agg((F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"))
    .filter(F.col("credit_limit_range") < 80)
    .select("country", "segment", "credit_limit_range")
    .orderBy("credit_limit_range")
)
