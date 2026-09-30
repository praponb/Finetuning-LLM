from pyspark.sql import functions as F

src = df_customers.filter(F.col("age") == 61)

customers_stats = (
    src
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.count("age").alias("age_count"),
    )
)

result = customers_stats.select("signup_quarter", "stddev_credit_limit", "age_count")
