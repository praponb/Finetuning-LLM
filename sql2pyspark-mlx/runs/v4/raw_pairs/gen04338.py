from pyspark.sql import functions as F

enriched = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.email"), F.col("b.city"), F.col("b.credit_limit"))
)

result = (
    enriched
    .withColumn("activity_month", F.date_trunc("MONTH", "end_date"))
    .rollup("activity_month")
    .agg(
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.max("credit_limit").alias("max_credit_limit"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.grouping("activity_month").alias("grouping_id"),
    )
)
