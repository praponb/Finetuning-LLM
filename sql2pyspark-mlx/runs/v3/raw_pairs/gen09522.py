from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Gra%") & (F.col("credit_limit") >= 8400))
    .groupBy(
        F.when(F.col("credit_limit") >= 13800, "high").when(F.col("credit_limit") >= 4000, "mid").otherwise("low").alias("credit_limit_bucket"),
        "country",
    )
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .filter(F.col("total_credit_limit") < 62000)
)
