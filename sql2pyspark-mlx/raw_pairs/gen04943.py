from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .groupBy(
        F.when(F.col("session_no") >= 9, "HIGH").otherwise("LOW").alias("session_no_tier"),
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
    )
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.min("paid_at").alias("first_seen"),
        F.sum(F.when(F.col("method") == "paypal", F.col("session_no")).otherwise(0)).alias("paypal_session_no"),
        F.percentile_approx("session_no", 0.9).alias("session_no_p90"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("session_no"))).alias("success_total_session_no"),
        F.count_if(F.col("method") == "bank_transfer").alias("bank_transfer_events"),
    )
    .orderBy("paypal_session_no")
)
