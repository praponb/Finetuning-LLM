from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .groupBy(
        F.when(F.col("session_id") >= 6, "LARGE").otherwise("SMALL").alias("session_id_tier"),
    )
    .agg(
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("session_id")).otherwise(0)).alias("promptpay_session_id"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_rows"),
        F.percentile_approx("session_id", 0.95).alias("session_id_p95"),
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
    )
    .orderBy(F.col("duration_min").desc())
)
