from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("is_new_session", "id")
w4 = Window.partitionBy(F.month("paid_at"), "method").orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status", "order_id").orderBy("session_id", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("order_id").orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("prev_session_id", "id")
w9 = Window.partitionBy("method").orderBy("running_avg_is_new_session", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.partitionBy("status", "method").orderBy("last4_min_running_avg_is_new_session", "id")
w11 = Window.orderBy(F.col("last4_min_running_avg_is_new_session").desc_nulls_last())

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

enriched = (
    sessions_payments
    .select(
        "*",
        F.lag("session_id", 1).over(w3).alias("prev_session_id"),
        F.sum("is_new_session").over(w4).alias("running_is_new_session"),
        F.avg("is_new_session").over(w5).alias("is_new_session_avg_6_prior"),
        F.last("method", ignorenulls=True).over(w6).alias("last_known_method"),
    )
)

enriched_payments = (
    enriched
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.order_date"))
)

enriched_payments_2 = (
    enriched_payments
    .filter(F.col("order_id").isin([10, 11, 39, 40]))
    .select(
        "id",
        "method",
        "status",
        "session_id",
        "prev_session_id",
        "running_is_new_session",
        "is_new_session_avg_6_prior",
        "last_known_method",
        F.min("is_new_session").over(w7).alias("running_min_is_new_session"),
        F.avg("is_new_session").over(w7).alias("running_avg_is_new_session"),
        F.sum("is_new_session").over(w7).alias("running_sum_is_new_session"),
        F.max("is_new_session").over(w7).alias("running_max_is_new_session"),
        F.lead("prev_session_id", 2).over(w8).alias("next_prev_session_id"),
    )
)

with_windows = (
    enriched_payments_2
    .select(
        "id",
        "method",
        "status",
        "session_id",
        "running_min_is_new_session",
        "next_prev_session_id",
        F.max("running_avg_is_new_session").over(w9).alias("last4_max_running_avg_is_new_session"),
        F.count("running_avg_is_new_session").over(w9).alias("last4_count_running_avg_is_new_session"),
        F.sum("running_avg_is_new_session").over(w9).alias("last4_sum_running_avg_is_new_session"),
        F.min("running_avg_is_new_session").over(w9).alias("last4_min_running_avg_is_new_session"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "status",
        "session_id",
        "running_min_is_new_session",
        "last4_min_running_avg_is_new_session",
        F.lead("last4_max_running_avg_is_new_session").over(w10).alias("next_last4_max_running_avg_is_new_session"),
        F.lag("next_prev_session_id").over(w10).alias("prev_next_prev_session_id"),
        F.lead("next_prev_session_id").over(w10).alias("next_next_prev_session_id"),
        F.row_number().over(w10).alias("event_seq"),
        F.percent_rank().over(w11).alias("last4_min_running_avg_is_new_session_percent_rank"),
    )
)
