from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 35) & F.col("status").isin(["EXPIRED", "ACTIVE"]))
    .groupBy("status")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.count("*").alias("row_count"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
    .orderBy("row_count")
)
