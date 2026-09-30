from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy("start_date", "id")
w2 = Window.partitionBy(F.year("end_date"), "customer_id").orderBy("end_date", "id")
w3 = Window.partitionBy("customer_id").orderBy("next_days_start_date_to_end_date_2", "id")
w4 = Window.partitionBy("status").orderBy("days_start_date_to_end_date", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy(F.col("prev_days_start_date_to_end_date").desc_nulls_last())
w_part = Window.partitionBy("status").orderBy("prev_days_start_date_to_end_date", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("status", "customer_id").orderBy("prev_next_days_start_date_to_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("last7_sum_days_start_date_to_end_date", "id")

filtered = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 160)
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "customer_id",
        "status",
        "days_start_date_to_end_date",
        F.lag("days_start_date_to_end_date").over(w_ordered).alias("prev_days_start_date_to_end_date"),
        F.lead("days_start_date_to_end_date").over(w_ordered).alias("next_days_start_date_to_end_date"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.lead("days_start_date_to_end_date", 2, 0).over(w2).alias("next_days_start_date_to_end_date_2"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.lag("next_days_start_date_to_end_date", 1).over(w3).alias("prev_next_days_start_date_to_end_date"),
        F.avg("next_days_start_date_to_end_date").over(w4).alias("next_days_start_date_to_end_date_avg_3"),
        F.rank().over(w5).alias("prev_days_start_date_to_end_date_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "prev_days_start_date_to_end_date",
        "next_days_start_date_to_end_date",
        "row_seq",
        "next_days_start_date_to_end_date_2",
        "prev_next_days_start_date_to_end_date",
        "next_days_start_date_to_end_date_avg_3",
        "prev_days_start_date_to_end_date_rank",
        F.max("days_start_date_to_end_date").over(w_part).alias("last7_max_days_start_date_to_end_date"),
        F.count("days_start_date_to_end_date").over(w_part).alias("last7_count_days_start_date_to_end_date"),
        F.sum("days_start_date_to_end_date").over(w_part).alias("last7_sum_days_start_date_to_end_date"),
        F.avg("days_start_date_to_end_date").over(w_part).alias("last7_avg_days_start_date_to_end_date"),
        F.first("prev_days_start_date_to_end_date_rank", ignorenulls=True).over(w_grp).alias("first_known_prev_days_start_date_to_end_date_rank"),
    )
)

metrics_subscriptions = (
    metrics
    .filter(F.col("prev_days_start_date_to_end_date_rank") > 4)
    .select(
        "id",
        "prev_days_start_date_to_end_date",
        "row_seq",
        "prev_days_start_date_to_end_date_rank",
        "last7_count_days_start_date_to_end_date",
        F.lead("next_days_start_date_to_end_date_avg_3").over(w_recent).alias("next_next_days_start_date_to_end_date_avg_3"),
        F.lag("next_days_start_date_to_end_date_avg_3").over(w_recent).alias("prev_next_days_start_date_to_end_date_avg_3"),
        F.lag("next_days_start_date_to_end_date").over(w_recent).alias("prev_next_days_start_date_to_end_date_2"),
        F.lead("next_days_start_date_to_end_date").over(w_recent).alias("next_next_days_start_date_to_end_date"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

valid = (
    metrics_subscriptions
    .filter(F.col("next_next_days_start_date_to_end_date_avg_3").between(24, 59))
    .withColumn("prev_days_start_date_to_end_date_rank_str", F.col("prev_days_start_date_to_end_date_rank").cast("string"))
)

result = (
    valid
    .select(
        "id",
        "row_seq",
        "prev_days_start_date_to_end_date_rank",
        "last7_count_days_start_date_to_end_date",
        "prev_next_days_start_date_to_end_date_2",
        "next_next_days_start_date_to_end_date",
        "seq_no",
        "prev_days_start_date_to_end_date_rank_str",
    )
    .orderBy("seq_no")
)
