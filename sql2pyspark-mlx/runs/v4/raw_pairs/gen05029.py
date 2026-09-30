from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "TH") | F.col("email").like("%@outlook.com"))
    .groupBy(
        F.when(F.col("credit_limit") >= 17100, "high").when(F.col("credit_limit") >= 6100, "mid").otherwise("low").alias("credit_limit_bucket"),
        "segment",
    )
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .filter(F.col("avg_credit_limit") >= 42)
    .orderBy("min_signup_date")
)
