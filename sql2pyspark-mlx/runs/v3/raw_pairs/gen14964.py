from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") > 38)
    .groupBy("city")
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.count("*").alias("customer_count"),
    )
    .filter(F.col("customer_count") >= 1)
    .orderBy(F.col("customer_count").desc())
)
