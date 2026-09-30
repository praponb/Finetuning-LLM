from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("method").orderBy("is_new_session", "id")
w_recent = Window.partitionBy("method").orderBy("prev_burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("prev_is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("prev_is_new_session", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy(F.to_date("prev_paid_at"), "method")
w_running = Window.partitionBy("status").orderBy("prev_paid_at_period", "id")
w9 = Window.partitionBy(F.to_date("prev_paid_at"), "status").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-13 00:00:00").cast("timestamp")) & (F.col("order_id") == 31))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.concat("method", F.lit("/"), "status").alias("method_status"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

with_prev = base.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

valid = (
    islands
    .filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
    .withColumn("prev_paid_at_period", F.date_trunc("WEEK", "prev_paid_at"))
)

metrics = (
    valid
    .withColumn("next_burst_id", F.lead("burst_id").over(w_part))
    .withColumn("prev_burst_id", F.lag("burst_id").over(w_part))
    .withColumn("next_is_new_session", F.lead("is_new_session").over(w_part))
    .withColumn("prev_is_new_session", F.lag("is_new_session").over(w_part))
    .withColumn("seq_no", F.row_number().over(w_part))
)

metrics_payments = (
    metrics
    .withColumn("last_known_prev_is_new_session", F.last("prev_is_new_session", ignorenulls=True).over(w_recent))
    .withColumn("first_known_is_new_session", F.first("is_new_session", ignorenulls=True).over(w5))
    .withColumn("prev_burst_id_avg_3", F.avg("prev_burst_id").over(w6))
    .withColumn("prev_paid_at_to_date_max_burst_id", F.max("burst_id").over(w7))
)

result = (
    metrics_payments
    .select(
        "id",
        "method",
        "paid_at",
        "method_clean",
        "prev_paid_at",
        "next_burst_id",
        "prev_is_new_session",
        "first_known_is_new_session",
        "prev_burst_id_avg_3",
        F.lead("prev_is_new_session").over(w_running).alias("next_prev_is_new_session"),
        F.lag("prev_burst_id").over(w_running).alias("prev_prev_burst_id"),
        F.lead("prev_burst_id").over(w_running).alias("next_prev_burst_id"),
        F.row_number().over(w_running).alias("row_seq"),
        F.last("prev_is_new_session", ignorenulls=True).over(w9).alias("last_known_prev_is_new_session_2"),
    )
)
