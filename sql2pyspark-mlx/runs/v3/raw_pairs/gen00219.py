from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg((F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"))
    .filter(F.col("credit_limit_range") >= 63)
    .orderBy("country")
    .limit(20)
)
