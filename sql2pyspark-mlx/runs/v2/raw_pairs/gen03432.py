from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .filter(F.col("max_credit_limit") > 6200)
    .drop("max_credit_limit")
    .orderBy("city")
)
