from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("email").like("%rac%") & (F.lower("email").like("%vy@%") | F.col("city").isin(["New York", "Singapore", "Berlin"])))
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .filter(F.col("stddev_age") >= 66)
    .orderBy("max_credit_limit")
)
