from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("amount_band", F.when(F.col("amount") >= 1390, "High").when(F.col("amount") >= 280, "Medium").otherwise("Low"))
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .groupBy(
        "order_id",
        F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL").alias("is_new_session_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.avg(F.when(F.col("method") == "paypal", F.col("amount"))).alias("paypal_avg_amount"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
    )
)
