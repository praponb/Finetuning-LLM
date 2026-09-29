from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.when(F.col("credit_limit") >= 12500, "high").when(F.col("credit_limit") >= 4400, "mid").otherwise("low").alias("credit_limit_bucket"))
    .pivot("country", ["DE", "TH", "UK", "US"])
    .agg(F.count("credit_limit"))
)
