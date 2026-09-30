from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("amount_tier").orderBy(F.col("promptpay_is_new_session").desc())

base = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("paid_at") >= F.lit("2023-05-16 18:00:00").cast("timestamp")))
    .select("*", F.concat("method", F.lit("/"), "status").alias("method_status"))
)

flagged = (
    base
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("amount") >= 1490, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
    )
    .agg(
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("is_new_session")).otherwise(0)).alias("promptpay_is_new_session"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
    )
    .filter(F.col("bank_transfer_rows") >= 1)
    .withColumn("promptpay_is_new_session_rank", F.rank().over(w3))
)
