from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("is_new_session") >= 1, "HIGH").otherwise("LOW").alias("is_new_session_tier"),
        F.date_trunc("week", "paid_at").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("method") == "card").alias("card_events"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("burst_id")).otherwise(0)).alias("success_burst_id"),
        F.max(F.when(F.col("method") == "promptpay", F.col("is_new_session"))).alias("promptpay_max_is_new_session"),
    )
    .filter(F.col("card_events") >= 1)
    .orderBy("success_burst_id")
)
