from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "enterprise", "standard"]) & F.col("customer_id").isin([21, 27]))
    .withColumn("monthly_fee_whole", F.ceil("monthly_fee"))
)

agg = (
    recent_subscriptions
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        (F.max("monthly_fee_whole") - F.min("monthly_fee_whole")).alias("monthly_fee_whole_range"),
    )
)

result = (
    agg
    .filter(F.col("monthly_fee_whole_range") >= 57)
    .select("status", "status_list", "monthly_fee_whole_range")
    .orderBy("status")
)
