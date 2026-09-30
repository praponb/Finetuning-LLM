from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("session_id_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("session_id_2")
w5 = Window.partitionBy("method").orderBy("amount", "id")
w6 = Window.partitionBy("method")
w7 = Window.partitionBy("status")
w_ordered = Window.partitionBy("order_id").orderBy("session_id_2_percent_rank", "id")
w9 = Window.partitionBy("status").orderBy("prev_paid_at", "id")
w10 = Window.partitionBy("status").orderBy("prev_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

with_gaps = (
    sessionized
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized_payments = with_gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

metrics = (
    sessionized_payments
    .withColumn("running_amount", F.sum("amount").over(w3))
    .withColumn("session_id_2_percent_rank", F.percent_rank().over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("method_avg_new_group_flag", F.avg("new_group_flag").over(w6))
    .withColumn("status_sum_session_id", F.sum("session_id").over(w7))
)

with_windows = (
    metrics
    .select(
        "id",
        "method",
        "amount",
        "status",
        "prev_paid_at",
        "new_group_flag",
        "session_id_2",
        F.lag("seq_no").over(w_ordered).alias("prev_seq_no"),
        F.lead("seq_no").over(w_ordered).alias("next_seq_no"),
        F.lead("session_id_2").over(w_ordered).alias("next_session_id_2"),
        F.lag("session_id_2").over(w_ordered).alias("prev_session_id_2"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

flagged_2 = (
    with_windows
    .withColumn("is_new_session", F.when(F.lag("prev_paid_at").over(w9).isNull() | (F.col("prev_paid_at") > F.lag("prev_paid_at").over(w9) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged_2.withColumn("island_id", F.sum("is_new_session").over(w10))

prev_events = sessions_payments.withColumn("prev_prev_paid_at", F.lag("prev_paid_at").over(w9))

with_gaps_2 = (
    prev_events
    .withColumn("new_group_flag_2", F.when(F.col("prev_prev_paid_at").isNull() | (F.col("prev_paid_at") > F.col("prev_prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized_payments_2 = (
    with_gaps_2
    .select(
        "id",
        "status",
        "prev_paid_at",
        "new_group_flag",
        "prev_seq_no",
        "next_seq_no",
        "next_session_id_2",
        "row_seq",
        "is_new_session",
        "island_id",
        F.sum("new_group_flag_2").over(w10).alias("session_no"),
    )
)

result = (
    sessionized_payments_2
    .select(
        "id",
        "prev_paid_at",
        "new_group_flag",
        "prev_seq_no",
        "next_seq_no",
        "next_session_id_2",
        "row_seq",
        "is_new_session",
        "island_id",
    )
    .orderBy(F.col("next_seq_no").desc())
)
