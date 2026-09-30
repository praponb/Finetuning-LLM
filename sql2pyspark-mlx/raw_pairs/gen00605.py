from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("status").orderBy("session_no", "id")

gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    gaps
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

with_gaps = (
    sessions_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_payments_2 = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

with_windows = (
    sessions_payments_2
    .withColumn("next_session_no", F.lead("session_no").over(w_recent))
    .withColumn("prev_session_no", F.lag("session_no").over(w_recent))
    .withColumn("prev_amount", F.lag("amount").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

base = (
    with_windows
    .filter(F.col("prev_amount") <= 1220)
    .select(
        "*",
        (F.col("next_session_no") - F.col("session_id")).alias("next_session_no_minus_session_id"),
        F.least("seq_no", "new_group_flag").alias("least_seq_no_new_group_flag"),
    )
)

result = (
    base
    .withColumn("year_start", F.date_trunc("year", "paid_at"))
    .withColumn("activity_month", F.date_trunc("month", "paid_at"))
    .withColumn("least_seq_no_new_group_flag_tier", F.when(F.col("least_seq_no_new_group_flag") >= 77, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["year_start", "activity_month", "least_seq_no_new_group_flag_tier"], ["year_start", "activity_month"]],
        "year_start",
        "activity_month",
        "least_seq_no_new_group_flag_tier",
    )
    .agg(
        F.sum("session_no").alias("total_session_no"),
        F.max("paid_at").alias("last_seen"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.avg(F.when(F.col("status") == "SUCCESS", F.col("prev_amount"))).alias("success_avg_prev_amount"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("success_avg_prev_amount") >= 0)
    .orderBy(F.col("least_seq_no_new_group_flag_tier").desc())
)
