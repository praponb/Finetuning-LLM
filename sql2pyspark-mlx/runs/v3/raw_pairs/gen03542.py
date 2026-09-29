from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~(F.col("credit_limit").between(7400, 11500) & (F.lower(F.trim("email")) == "bob.smith@yahoo.com")))
    .groupBy(
        F.when(F.col("age") >= 60, "high").when(F.col("age") >= 37, "mid").otherwise("low").alias("age_bucket"),
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
    )
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.avg("age").alias("avg_age"),
        F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"),
    )
    .filter(F.col("th_count") >= 24)
    .drop("th_count")
    .orderBy("age_bucket", "signup_month")
    .limit(5)
)
