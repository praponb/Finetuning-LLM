from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & (F.month("start_date").isin([1, 3, 12]) | (F.col("start_date") >= "2024-02-26")))
    .rollup(
        F.when(F.col("status").isin(["CANCELLED", "PAUSED"]), "tier_1").otherwise("other").alias("status_group"),
        F.month("start_date").alias("start_month"),
    )
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
    .orderBy(F.col("start_month").desc(), "status_group")
)
