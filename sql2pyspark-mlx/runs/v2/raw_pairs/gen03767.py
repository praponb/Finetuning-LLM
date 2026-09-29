from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 23)
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(F.sort_array(F.collect_set("plan")).alias("plan_list"))
    .orderBy("start_month")
    .limit(50)
)
