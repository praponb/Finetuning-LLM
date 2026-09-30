from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & ((F.year("start_date") >= 2024) | (F.col("start_date") >= "2023-10-01")))
)

status_summary = (
    filtered_subscriptions
    .groupBy("status", "plan")
    .agg(F.sort_array(F.collect_set("plan")).alias("plan_list"))
)

result = (
    status_summary
    .select("status", "plan", "plan_list")
    .orderBy("plan", "status")
    .limit(20)
)
