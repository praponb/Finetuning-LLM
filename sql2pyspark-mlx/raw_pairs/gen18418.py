from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")
w2 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("status").orderBy("last_known_monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("plan").orderBy(F.col("prev_plan_length").desc_nulls_first())
w5 = Window.partitionBy("plan", "status").orderBy("row_seq", "id")
w6 = Window.partitionBy("status").orderBy("row_seq", "id")
w7 = Window.orderBy("prev_plan_length_avg_3", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("prev_plan_length_rank", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.orderBy("running_prev_plan_length_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("last4_max_prev_monthly_fee", "id")
w12 = Window.orderBy("row_seq_2", "id")

base = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNull())
    .select("*", F.length("plan").alias("plan_length"))
)

enriched = (
    base
    .filter(F.col("plan_length").between(50, 78))
    .select(
        "id",
        "plan",
        "status",
        F.lag("plan_length").over(w).alias("prev_plan_length"),
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.row_number().over(w).alias("row_seq"),
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "prev_monthly_fee",
        F.avg("prev_plan_length").over(w_recent).alias("prev_plan_length_avg_3"),
        F.rank().over(w_grp).alias("prev_plan_length_rank"),
        F.row_number().over(w5).alias("row_seq_2"),
        F.lead("last_known_monthly_fee", 1).over(w6).alias("next_last_known_monthly_fee"),
    )
)

enriched_subscriptions = (
    metrics
    .select(
        "id",
        "prev_monthly_fee",
        "prev_plan_length_rank",
        "row_seq_2",
        F.avg("next_last_known_monthly_fee").over(w7).alias("next_last_known_monthly_fee_avg_2"),
        F.sum("prev_plan_length_rank").over(w8).alias("running_prev_plan_length_rank"),
    )
)

enriched_subscriptions_2 = (
    enriched_subscriptions
    .select(
        "*",
        F.min("prev_monthly_fee").over(w9).alias("last4_min_prev_monthly_fee"),
        F.count("prev_monthly_fee").over(w9).alias("last4_count_prev_monthly_fee"),
        F.max("prev_monthly_fee").over(w9).alias("last4_max_prev_monthly_fee"),
        F.sum("prev_plan_length_rank").over(w10).alias("running_prev_plan_length_rank_2"),
    )
)

result = (
    enriched_subscriptions_2
    .select(
        "id",
        "next_last_known_monthly_fee_avg_2",
        "running_prev_plan_length_rank",
        "last4_min_prev_monthly_fee",
        "running_prev_plan_length_rank_2",
        F.lag("prev_plan_length_rank", 1, 0).over(w11).alias("prev_prev_plan_length_rank"),
        F.lag("prev_monthly_fee", 1).over(w12).alias("prev_prev_monthly_fee"),
    )
)
