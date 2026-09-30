from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
    )
    .filter(F.col("stddev_age") >= 48)
)
