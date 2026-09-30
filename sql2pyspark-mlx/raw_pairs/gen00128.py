from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-10-02 08:00:00")))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.dayofmonth("paid_at").alias("paid_at_day"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "paid_at_day",
        "method_filled",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessions_payments
    .groupBy("order_id", F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_rows"),
        F.min("paid_at").alias("first_seen"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("session_id_per_refunded"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
        F.avg(F.when(F.col("method") == "cash", F.col("paid_at_day"))).alias("cash_avg_paid_at_day"),
    )
    .filter(F.col("session_id_per_refunded") > 0)
)
