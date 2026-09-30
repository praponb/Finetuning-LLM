from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("start_date")).orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy(F.date_trunc("WEEK", "end_date"), "customer_id")
w4 = Window.partitionBy("status").orderBy("last_known_monthly_fee", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy(F.year("end_date"), "status").orderBy("last3_sum_end_date_period_max_monthly_fee", "id")
w6 = Window.orderBy("last3_avg_end_date_period_max_monthly_fee", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy(F.date_trunc("WEEK", "start_date"))
w8 = Window.partitionBy(F.date_trunc("WEEK", "start_date")).orderBy("last3_avg_end_date_period_max_monthly_fee_avg_5_prior", "id")
w9 = Window.partitionBy(F.date_trunc("MONTH", "start_date")).orderBy("cumulative_monthly_fee", "id")

metrics = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.last("monthly_fee", ignorenulls=True).over(w).alias("last_known_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
        F.max("monthly_fee").over(w_ordered).alias("end_date_period_max_monthly_fee"),
    )
)

metrics_subscriptions = (
    metrics
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "last_known_monthly_fee",
        "cumulative_monthly_fee",
        F.sum("end_date_period_max_monthly_fee").over(w4).alias("last3_sum_end_date_period_max_monthly_fee"),
        F.avg("end_date_period_max_monthly_fee").over(w4).alias("last3_avg_end_date_period_max_monthly_fee"),
        F.count("end_date_period_max_monthly_fee").over(w4).alias("last3_count_end_date_period_max_monthly_fee"),
    )
)

metrics_subscriptions_2 = (
    metrics_subscriptions
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "last_known_monthly_fee",
        "cumulative_monthly_fee",
        "last3_count_end_date_period_max_monthly_fee",
        F.row_number().over(w5).alias("row_seq"),
        F.avg("last3_avg_end_date_period_max_monthly_fee").over(w6).alias("last3_avg_end_date_period_max_monthly_fee_avg_5_prior"),
    )
)

with_windows = (
    metrics_subscriptions_2
    .filter(F.col("cumulative_monthly_fee") > 831)
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w7)), 4).alias("monthly_fee_share"),
        F.lag("monthly_fee", 2).over(w8).alias("prev_monthly_fee"),
        F.ntile(5).over(w9).alias("cumulative_monthly_fee_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "last_known_monthly_fee",
        "last3_count_end_date_period_max_monthly_fee",
        "row_seq",
        "monthly_fee_share",
        "prev_monthly_fee",
        "cumulative_monthly_fee_bucket",
    )
    .orderBy("id", F.col("monthly_fee_share").desc())
    .limit(10)
)
