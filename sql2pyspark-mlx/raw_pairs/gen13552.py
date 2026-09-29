from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("end_date"), "plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("cumulative_days_since_end_date", "id")
w4 = Window.orderBy("first_known_monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy(F.col("first_known_monthly_fee").desc(), "id")

base = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-10-07").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
        F.round("monthly_fee", 2).alias("monthly_fee_rounded"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "start_date",
        F.sum("days_since_end_date").over(w).alias("cumulative_days_since_end_date"),
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
    )
)

base_subscriptions = (
    with_windows
    .filter(F.col("start_date") > F.lit("2023-08-16").cast("date"))
    .withColumn("cumulative_days_since_end_date_filled", F.coalesce("cumulative_days_since_end_date", F.lit(0)))
    .withColumn("start_date_dayofweek", F.dayofweek("start_date"))
)

with_windows_subscriptions = (
    base_subscriptions
    .withColumn("prev_cumulative_days_since_end_date", F.lag("cumulative_days_since_end_date").over(w3))
    .withColumn("prev_start_date_dayofweek", F.lag("start_date_dayofweek").over(w3))
    .withColumn("seq_no", F.row_number().over(w3))
)

scored = (
    with_windows_subscriptions
    .filter(F.col("first_known_monthly_fee") < 220)
    .select(
        "id",
        "start_date",
        "cumulative_days_since_end_date_filled",
        "prev_start_date_dayofweek",
        F.sum("cumulative_days_since_end_date_filled").over(w4).alias("last4_sum_cumulative_days_since_end_date_filled"),
        F.min("cumulative_days_since_end_date_filled").over(w4).alias("last4_min_cumulative_days_since_end_date_filled"),
        F.max("cumulative_days_since_end_date_filled").over(w4).alias("last4_max_cumulative_days_since_end_date_filled"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

filtered = (
    scored
    .filter(F.col("last4_min_cumulative_days_since_end_date_filled") <= 50)
    .withColumn("start_date_month_end", F.last_day("start_date"))
)

result = (
    filtered
    .select(
        "id",
        "start_date",
        "last4_sum_cumulative_days_since_end_date_filled",
        "last4_min_cumulative_days_since_end_date_filled",
        "last4_max_cumulative_days_since_end_date_filled",
        "start_date_month_end",
    )
    .orderBy("last4_max_cumulative_days_since_end_date_filled")
)
