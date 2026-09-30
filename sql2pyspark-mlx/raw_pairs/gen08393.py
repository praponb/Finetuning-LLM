from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([2, 8, 12]))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"), "segment")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.count("*").alias("customer_count"),
    )
    .filter(F.col("customer_count") > 6)
    .select("signup_month", "segment", "stddev_age", "customer_count")
    .orderBy("segment", "signup_month")
    .limit(10)
)
