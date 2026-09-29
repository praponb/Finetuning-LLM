from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("country")
    .pivot("city", ["Berlin", "Bangkok", "Tokyo"])
    .agg(F.avg("credit_limit"))
    .orderBy("country")
)
