from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") != "enterprise") & F.col("start_date").between("2023-04-21", "2024-12-01") & (F.col("monthly_fee") < 100))
    .groupBy("status", "customer_id")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
    .filter(F.col("stddev_monthly_fee") > 77)
    .orderBy("customer_id")
)
