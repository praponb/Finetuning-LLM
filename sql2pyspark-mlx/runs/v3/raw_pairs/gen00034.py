from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.round(((100 * F.sum(F.when(F.col("country") == "US", 1).otherwise(0))) / F.count("*")), 1).alias("pct_us"),
    )
    .orderBy("avg_credit_limit")
)
