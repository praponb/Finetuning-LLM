from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.country").isin(["US", "TH"]))
    .groupBy(
        F.when(F.col("c.age") >= 70, "high").when(F.col("c.age") >= 27, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.count(F.when(F.col("s.country") == "UK", 1)).alias("uk_count"))
)
