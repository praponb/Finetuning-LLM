from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w_recent = Window.partitionBy(F.month("start_date"), "customer_id").orderBy("start_date", "id")
w_recent_2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy("plan", "customer_id").orderBy("monthly_fee", "id")
w_ordered = Window.orderBy("monthly_fee", "id")
w6 = Window.partitionBy("status").orderBy(F.col("monthly_fee_bucket").asc_nulls_last())
w7 = Window.partitionBy("subscription_plan")
w8 = Window.partitionBy("subscription_plan_2", "subscription_plan").orderBy("start_date", "id", "subscription_id", "subscription_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("subscription_plan_2").orderBy("monthly_fee_bucket", "id", "subscription_id", "subscription_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("status", "plan").orderBy(F.col("monthly_fee_bucket_percent_rank").desc_nulls_first(), "id", "subscription_id", "subscription_id_2")

scored = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.ntile(4).over(w_recent_2).alias("monthly_fee_bucket"),
        F.lag("monthly_fee", 1).over(w4).alias("prev_monthly_fee"),
        F.row_number().over(w_ordered).alias("event_seq_2"),
    )
)

with_window = (
    scored
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee"),
    )
)

matched = (
    with_window
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

scored_subscriptions = (
    matched
    .select(
        "*",
        F.percent_rank().over(w6).alias("monthly_fee_bucket_percent_rank"),
        F.round((F.col("prev_monthly_fee") / F.sum("prev_monthly_fee").over(w7)), 4).alias("prev_monthly_fee_share"),
        F.sum("monthly_fee").over(w8).alias("cumulative_monthly_fee"),
    )
)

with_windows = (
    scored_subscriptions
    .select(
        "id",
        "plan",
        "status",
        "monthly_fee_share",
        "event_seq",
        "prev_monthly_fee",
        "subscription_id",
        "monthly_fee",
        "subscription_id_2",
        "monthly_fee_bucket_percent_rank",
        "cumulative_monthly_fee",
        F.min("subscription_id").over(w_part).alias("running_min_subscription_id"),
        F.avg("subscription_id").over(w_part).alias("running_avg_subscription_id"),
        F.max("subscription_id").over(w_part).alias("running_max_subscription_id"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w10))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    latest
    .filter(F.col("running_min_subscription_id") <= 6.9)
    .withColumn("status_title", F.initcap(F.trim("status")))
)

result = (
    cleaned
    .select(
        "id",
        "plan",
        "status",
        "monthly_fee_share",
        "prev_monthly_fee",
        "subscription_id",
        "monthly_fee",
        "subscription_id_2",
        "running_min_subscription_id",
        F.when(F.col("plan").rlike("^(enterprise|premium)$"), "CORE").when(F.col("plan").rlike("^(basic|standard)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("plan_zone"),
    )
)
