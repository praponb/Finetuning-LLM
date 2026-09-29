from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("monthly_fee").asc_nulls_first())
w_recent = Window.partitionBy("plan", "status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("customer_id").orderBy(F.col("dedup_rank").desc(), "id")
w5 = Window.partitionBy("plan", "customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id", "status").orderBy(F.col("running_avg_monthly_fee").desc(), "id")
w7 = Window.partitionBy(F.year("start_date"), "plan")
w_ordered = Window.partitionBy("status").orderBy("cumulative_monthly_fee", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("customer_id", "status").orderBy("event_seq_2", "id")

latest = (
    df_subscriptions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .filter(F.col("dedup_rank") == 1)
    .select(
        "*",
        F.avg("monthly_fee").over(w_recent).alias("running_avg_monthly_fee"),
        F.min("monthly_fee").over(w_recent).alias("running_min_monthly_fee"),
        F.count("monthly_fee").over(w_recent).alias("running_count_monthly_fee"),
        F.min("monthly_fee").over(w3).alias("plan_min_monthly_fee"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        "dedup_rank",
        F.row_number().over(w4).alias("event_seq"),
        F.sum("monthly_fee").over(w5).alias("cumulative_monthly_fee"),
        F.row_number().over(w6).alias("event_seq_2"),
        F.count("running_count_monthly_fee").over(w7).alias("start_date_year_count_running_count_monthly_fee"),
    )
)

with_windows_subscriptions = (
    with_windows
    .select(
        "id",
        "customer_id",
        "status",
        "dedup_rank",
        "cumulative_monthly_fee",
        "event_seq_2",
        "start_date_year_count_running_count_monthly_fee",
        F.avg("event_seq_2").over(w_ordered).alias("last7_avg_event_seq_2"),
        F.sum("event_seq_2").over(w_ordered).alias("last7_sum_event_seq_2"),
        F.min("event_seq_2").over(w_ordered).alias("last7_min_event_seq_2"),
        F.ntile(5).over(w9).alias("event_seq_2_bucket"),
    )
)

result = (
    with_windows_subscriptions
    .withColumn("start_date_year_count_running_count_monthly_fee_tier", F.when(F.col("start_date_year_count_running_count_monthly_fee") >= 17, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["start_date_year_count_running_count_monthly_fee_tier", "status"], ["start_date_year_count_running_count_monthly_fee_tier"]],
        "start_date_year_count_running_count_monthly_fee_tier",
        "status",
    )
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("last7_avg_event_seq_2")).otherwise(0)).alias("active_last7_avg_event_seq_2"),
        F.avg("last7_sum_event_seq_2").alias("avg_last7_sum_event_seq_2"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.grouping_id().alias("grouping_level"),
    )
)
