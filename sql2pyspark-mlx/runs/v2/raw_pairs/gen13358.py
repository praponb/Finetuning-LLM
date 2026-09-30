from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["JP", "US", "SG"]) | F.col("country").isin(["SG", "UK", "DE"]))
    .groupBy(
        F.when(F.col("credit_limit") >= 15600, "high").when(F.col("credit_limit") >= 7100, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)
