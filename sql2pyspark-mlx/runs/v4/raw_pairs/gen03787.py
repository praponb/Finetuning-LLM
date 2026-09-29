from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2024-03-16 02:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.length("status").alias("status_length"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

prev_events = filtered.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy("method")
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("status_length")).otherwise(0)).alias("failed_status_length"),
        F.round(F.sum("is_new_session") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("is_new_session_per_failed"),
        F.count_if(F.col("status") == "SUCCESS").alias("success_events"),
    )
    .orderBy("is_new_session_per_failed")
)
