from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("running_sum_monthly_fee", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("plan").orderBy(F.col("running_avg_monthly_fee").desc())
w4 = Window.partitionBy(F.month("end_date"), "plan").orderBy(F.col("monthly_fee").desc(), "id")
w5 = Window.partitionBy("customer_id", "status")
w6 = Window.orderBy("seq_no", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.date_trunc("YEAR", "end_date"), "subscription_plan").orderBy("monthly_fee", "id", "subscription_id")
w8 = Window.partitionBy("plan", "subscription_plan").orderBy("seq_no", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("customer_id").orderBy(F.col("prev_running_avg_monthly_fee_rank").desc_nulls_first(), "id", "subscription_id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

with_windows = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.avg("monthly_fee").over(w).alias("running_avg_monthly_fee"),
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("running_sum_monthly_fee"),
    )
)

with_windows_subscriptions = (
    with_windows
    .filter(F.col("running_avg_monthly_fee") >= 90)
    .select(
        "*",
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_3_prior"),
        F.rank().over(w3).alias("running_avg_monthly_fee_rank"),
        F.row_number().over(w4).alias("seq_no"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w5)), 4).alias("monthly_fee_share"),
    )
)

with_window = (
    with_windows_subscriptions
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.end_date") >= F.col("sub.start_date")) & (F.col("src.end_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

enriched = (
    with_window
    .select(
        "*",
        F.first("seq_no", ignorenulls=True).over(w6).alias("first_known_seq_no"),
        F.lag("running_avg_monthly_fee_rank", 1).over(w7).alias("prev_running_avg_monthly_fee_rank"),
        F.sum("running_count_monthly_fee").over(w8).alias("running_running_count_monthly_fee"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        "running_avg_monthly_fee",
        "running_sum_monthly_fee",
        "monthly_fee_avg_3_prior",
        "running_avg_monthly_fee_rank",
        F.when(F.col("status").rlike("^(PAUSED|EXPIRED)$"), "CORE").when(F.col("status").rlike("^(ACTIVE|CANCELLED)$"), "GROUP_B").otherwise("GROUP_A").alias("status_zone"),
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy("running_avg_monthly_fee_rank", F.col("status_zone").desc())
)
