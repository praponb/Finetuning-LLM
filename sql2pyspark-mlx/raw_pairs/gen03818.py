from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        "status",
        F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL").alias("is_new_session_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("session_id")).otherwise(0)).alias("promptpay_session_id"),
    )
)
