from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("subscription_plan").orderBy("monthly_fee_rank", "id", "subscription_id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("subscription_plan").orderBy("start_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("customer_id").orderBy("running_monthly_fee", "id", "subscription_id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("plan", "status").orderBy("last7_min_subscription_monthly_fee", "id", "subscription_id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("status", "plan")
w8 = Window.partitionBy("plan").orderBy(F.col("last7_max_last3_sum_running_monthly_fee").desc_nulls_first(), "id", "subscription_id")

with_windows = (
    df_subscriptions
    .withColumn("monthly_fee_rank", F.rank().over(w))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w2))
)

with_window = (
    with_windows
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

with_windows_subscriptions = (
    with_window
    .select(
        "*",
        F.min("subscription_monthly_fee").over(w3).alias("last7_min_subscription_monthly_fee"),
        F.avg("subscription_monthly_fee").over(w3).alias("last7_avg_subscription_monthly_fee"),
        F.count("subscription_monthly_fee").over(w3).alias("last7_count_subscription_monthly_fee"),
        F.max("subscription_monthly_fee").over(w3).alias("last7_max_subscription_monthly_fee"),
        F.sum("monthly_fee").over(w4).alias("cumulative_monthly_fee"),
    )
)

scored = (
    with_windows_subscriptions
    .select(
        "id",
        "plan",
        "status",
        "monthly_fee_rank",
        "running_monthly_fee",
        "subscription_id",
        "subscription_plan",
        "subscription_monthly_fee",
        "last7_min_subscription_monthly_fee",
        "last7_avg_subscription_monthly_fee",
        F.sum("running_monthly_fee").over(w_running).alias("last3_sum_running_monthly_fee"),
        F.count("running_monthly_fee").over(w_running).alias("last3_count_running_monthly_fee"),
        F.min("running_monthly_fee").over(w_running).alias("last3_min_running_monthly_fee"),
    )
)

base = (
    scored
    .filter(F.col("subscription_plan").isin(["basic", "premium"]))
    .withColumn("last7_avg_subscription_monthly_fee_int", F.col("last7_avg_subscription_monthly_fee").cast("int"))
    .withColumn("subscription_plan_clean", F.coalesce("subscription_plan", F.lit("NONE")))
)

metrics = (
    base
    .select(
        "*",
        F.max("last3_sum_running_monthly_fee").over(w6).alias("last7_max_last3_sum_running_monthly_fee"),
        F.sum("last3_sum_running_monthly_fee").over(w6).alias("last7_sum_last3_sum_running_monthly_fee"),
        F.avg("last3_sum_running_monthly_fee").over(w6).alias("last7_avg_last3_sum_running_monthly_fee"),
        F.avg("last3_count_running_monthly_fee").over(w7).alias("status_avg_last3_count_running_monthly_fee"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "monthly_fee_rank",
        "running_monthly_fee",
        "subscription_id",
        "subscription_monthly_fee",
        "last7_avg_subscription_monthly_fee_int",
        "last7_sum_last3_sum_running_monthly_fee",
        "last7_avg_last3_sum_running_monthly_fee",
    )
)

result = (
    ranked
    .groupBy("subscription_id")
    .agg(
        F.percentile_approx("subscription_id", [0.25, 0.5, 0.75]).alias("subscription_id_quartiles"),
        F.percentile_approx("last7_avg_last3_sum_running_monthly_fee", 0.95).alias("last7_avg_last3_sum_running_monthly_fee_p95"),
    )
)
