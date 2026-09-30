from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("year_start").orderBy(F.col("session_no_p90").desc())

gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .groupBy(
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.percentile_approx("session_no", 0.9).alias("session_no_p90"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .withColumn("session_no_p90_rank", F.rank().over(w3))
)
