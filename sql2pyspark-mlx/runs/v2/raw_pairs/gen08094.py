from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["US", "TH", "UK"]))
    .groupBy(F.when(F.col("credit_limit") >= 17200, "high").when(F.col("credit_limit") >= 2800, "mid").otherwise("low").alias("credit_limit_bucket"))
    .pivot("city", ["Berlin", "Singapore", "New York", "Tokyo"])
    .agg(F.count("age"))
)
