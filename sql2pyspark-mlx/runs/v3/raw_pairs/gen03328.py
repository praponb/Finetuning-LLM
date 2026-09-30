from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") != "CANCELLED") & F.col("customer_id").isin([4, 30]))
    .groupBy("plan")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
)
