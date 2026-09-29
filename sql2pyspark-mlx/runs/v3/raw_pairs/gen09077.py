from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 52)
    .groupBy("is_active")
    .agg(
        F.count("*").alias("customer_count"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
)
