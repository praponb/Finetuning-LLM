from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(~((F.col("start_date") >= "2024-08-01") & ~F.col("status").isin(["PAUSED", "EXPIRED", "ACTIVE"])))
)

summary = (
    filtered_subscriptions
    .groupBy("plan", F.month("start_date").alias("start_month"))
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"))
)

result = summary
