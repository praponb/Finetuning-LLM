from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.when(F.col("age") >= 73, "high").when(F.col("age") >= 40, "mid").otherwise("low").alias("age_bucket"))
    .pivot("city", ["London", "Singapore"])
    .agg(F.avg("age"))
)
