from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg((F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"))
    .filter(F.col("credit_limit_range") > 56)
    .select("city", "credit_limit_range")
)
