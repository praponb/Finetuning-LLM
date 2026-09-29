from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter((F.col("start_date") <= "2025-10-01") & ((F.col("start_date") < "2024-05-07") | F.col("customer_id").isin([4, 30])))
)

summary = (
    recent_subscriptions
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("start_date").alias("min_start_date"),
    )
)

result = summary.orderBy("min_start_date")
