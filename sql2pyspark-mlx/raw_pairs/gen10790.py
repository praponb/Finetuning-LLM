from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("weeks_end_date_to_start_date", (F.datediff("start_date", "end_date") / 7))
)

result = (
    base
    .groupBy("customer_id")
    .agg(
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "PAUSED", 1)), F.lit(0)), 2).alias("monthly_fee_per_paused"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.avg("weeks_end_date_to_start_date"), 2).alias("avg_weeks_end_date_to_start_date"),
    )
    .orderBy("num_status")
)
