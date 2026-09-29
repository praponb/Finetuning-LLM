from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.col("status") == "EXPIRED")

start_year_summary = (
    filtered_subscriptions
    .groupBy(F.year("start_date").alias("start_year"))
    .agg(
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
)

result = (
    start_year_summary
    .filter(F.col("premium_count") >= 6)
    .orderBy("premium_count")
)
