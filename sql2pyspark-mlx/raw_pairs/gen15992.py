from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("monthly_fee")
w4 = Window.partitionBy(F.month("end_date"), "customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("plan").orderBy("monthly_fee_dense_rank")

enriched = (
    df_subscriptions
    .withColumn("monthly_fee_bucket", F.ntile(5).over(w))
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w2))
    .withColumn("monthly_fee_dense_rank", F.dense_rank().over(w3))
    .withColumn("first_known_monthly_fee", F.first("monthly_fee", ignorenulls=True).over(w4))
)

enriched_subscriptions = (
    enriched
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status",
        "monthly_fee_dense_rank",
        F.count("last_known_monthly_fee").over(w_ordered).alias("running_count_last_known_monthly_fee"),
        F.min("last_known_monthly_fee").over(w_ordered).alias("running_min_last_known_monthly_fee"),
        F.max("last_known_monthly_fee").over(w_ordered).alias("running_max_last_known_monthly_fee"),
    )
)

ranked = (
    enriched_subscriptions
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

attributed = (
    ranked
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee"),
    )
)

matched = (
    attributed
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.end_date") >= F.col("sub.start_date")) & (F.col("e.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    matched
    .alias("a")
    .join(df_customers.alias("dim"), F.col("a.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["country", "subscription_plan"], ["country"], []],
        F.col("dim.country").alias("country"),
        F.col("a.subscription_plan").alias("subscription_plan"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.running_min_last_known_monthly_fee")), 2).alias("avg_running_min_last_known_monthly_fee"),
        F.round(F.sum(F.col("a.running_count_last_known_monthly_fee")), 2).alias("total_running_count_last_known_monthly_fee"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("avg_running_min_last_known_monthly_fee")
)
