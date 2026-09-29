from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"))
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.percentile_approx("credit_limit", 0.95).alias("credit_limit_p95"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("activity_month").desc())
)
