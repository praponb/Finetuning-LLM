from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isin([13, 15, 17, 28]))
    .withColumn("plan_group", F.when(F.col("plan").isin(["premium", "standard"]), "Group A").otherwise("Other"))
)

start_year_summary = (
    filtered_subscriptions
    .groupBy(F.year("start_date").alias("start_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)

result = start_year_summary.filter(F.col("stddev_monthly_fee") >= 75)
