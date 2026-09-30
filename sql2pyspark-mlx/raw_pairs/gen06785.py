from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.partitionBy("customer_id").orderBy("start_date", "id")
w4 = Window.partitionBy(F.month("end_date"), "status")
w5 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_last(), "id", "subscription_id")
w_hist = Window.partitionBy("status")
w7 = Window.partitionBy(F.month("start_date")).orderBy("row_num", "id", "subscription_id")
w8 = Window.partitionBy("subscription_plan").orderBy("monthly_fee_cume_dist", "id", "subscription_id")
w9 = Window.partitionBy("status", "customer_id")
w10 = Window.partitionBy("customer_id", "plan")
w11 = Window.partitionBy(F.month("end_date"), "plan").orderBy("subscription_id", "id", "subscription_id").rowsBetween(-5, Window.currentRow)
w12 = Window.partitionBy(F.year("end_date"), "plan").orderBy("plan_sum_monthly_fee_share", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("subscription_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.orderBy("status_sum_event_seq_share", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("plan").orderBy(F.col("cumulative_seq_no").desc_nulls_first())

enriched = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(100, 220))
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("plan_sum_monthly_fee"),
        F.cume_dist().over(w2).alias("monthly_fee_cume_dist"),
        F.row_number().over(w3).alias("event_seq"),
        F.max("monthly_fee").over(w4).alias("end_date_month_max_monthly_fee"),
    )
)

matched = (
    enriched
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.start_date") >= F.col("sub.start_date")) & (F.col("e.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

latest = (
    matched
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "plan_sum_monthly_fee",
        "subscription_id",
        "row_num",
        F.sum("event_seq").over(w_hist).alias("status_sum_event_seq"),
        F.row_number().over(w7).alias("seq_no"),
        F.lead("monthly_fee", 1).over(w8).alias("next_monthly_fee"),
        F.round((F.col("plan_sum_monthly_fee") / F.sum("plan_sum_monthly_fee").over(w9)), 4).alias("plan_sum_monthly_fee_share"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "plan",
        "end_date",
        "subscription_id",
        "row_num",
        "next_monthly_fee",
        F.round((F.col("status_sum_event_seq") / F.sum("status_sum_event_seq").over(w10)), 4).alias("status_sum_event_seq_share"),
        F.avg("next_monthly_fee").over(w11).alias("next_monthly_fee_avg_5"),
        F.sum("seq_no").over(w12).alias("cumulative_seq_no"),
        F.sum("seq_no").over(w13).alias("cumulative_seq_no_2"),
    )
)

base = (
    with_windows
    .filter(F.col("cumulative_seq_no_2").isNull())
    .select(
        "id",
        "plan",
        "end_date",
        "subscription_id",
        "row_num",
        "next_monthly_fee",
        "status_sum_event_seq_share",
        "cumulative_seq_no",
        "cumulative_seq_no_2",
        (F.col("cumulative_seq_no") - F.col("row_num")).alias("cumulative_seq_no_minus_row_num"),
    )
)

with_windows_subscriptions = (
    base
    .select(
        "id",
        "plan",
        "subscription_id",
        "next_monthly_fee",
        F.sum("next_monthly_fee").over(w14).alias("running_next_monthly_fee"),
        F.rank().over(w_part).alias("cumulative_seq_no_rank"),
    )
)

result = (
    with_windows_subscriptions
    .select(
        "id",
        "plan",
        "subscription_id",
        "running_next_monthly_fee",
        "cumulative_seq_no_rank",
    )
    .orderBy(F.col("id").desc_nulls_last(), F.col("subscription_id").desc())
)
