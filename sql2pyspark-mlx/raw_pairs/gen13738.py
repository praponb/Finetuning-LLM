from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "Consumer"]))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "US", 1).otherwise(0))) / F.count("*")), 1).alias("pct_us"),
        F.min("signup_date").alias("min_signup_date"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
)
