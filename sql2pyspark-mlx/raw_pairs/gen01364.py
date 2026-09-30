from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.round(((100 * F.sum(F.when(F.col("country") == "UK", 1).otherwise(0))) / F.count("*")), 1).alias("pct_uk"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .orderBy(F.col("pct_uk").desc(), "is_active")
    .limit(50)
)
