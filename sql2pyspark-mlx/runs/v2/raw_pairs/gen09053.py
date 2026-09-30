from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 13300)
    .groupBy("country", "is_active")
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .filter(F.col("stddev_credit_limit") < 53)
    .orderBy("country")
)
