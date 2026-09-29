from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "premium") & F.col("end_date").between("2023-08-01", "2024-06-25"))
    .groupBy("customer_id")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
    .orderBy(F.col("customer_id").desc(), F.col("stddev_monthly_fee").desc())
    .limit(50)
)
