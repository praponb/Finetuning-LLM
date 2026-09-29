from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_first(), "id")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.date_trunc("MONTH", "end_date"), "plan").orderBy(F.col("months_since_end_date_minus_57d_avg_5_prior").desc(), "id")
w7 = Window.partitionBy("status").orderBy("first_known_days_since_end_date", "id")

ranked = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

filtered = (
    deduped
    .filter(F.col("customer_id").isNotNull() & F.col("end_date").between("2023-06-01", "2024-01-01"))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
        F.date_sub("end_date", 57).alias("end_date_minus_57d"),
    )
)

base = (
    filtered
    .filter(F.col("days_since_end_date") < 53)
    .select(
        "*",
        (F.datediff(F.current_date(), "end_date_minus_57d") / 30).alias("months_since_end_date_minus_57d"),
    )
)

scored = (
    base
    .select(
        "id",
        "plan",
        "end_date",
        "status",
        "months_since_end_date_minus_57d",
        F.max("days_since_end_date").over(w3).alias("status_max_days_since_end_date"),
        F.avg("months_since_end_date_minus_57d").over(w4).alias("months_since_end_date_minus_57d_avg_5_prior"),
        F.first("days_since_end_date", ignorenulls=True).over(w5).alias("first_known_days_since_end_date"),
    )
)

with_windows = (
    scored
    .filter(F.col("end_date").between("2023-11-01", "2024-03-01"))
    .select(
        "*",
        F.ntile(10).over(w6).alias("months_since_end_date_minus_57d_avg_5_prior_bucket"),
        F.lag("months_since_end_date_minus_57d", 2, 0).over(w7).alias("prev_months_since_end_date_minus_57d"),
    )
)

result = (
    with_windows
    .groupBy("plan")
    .agg(
        F.count_if(F.col("status") == "PAUSED").alias("paused_events"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
)
