from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("method").isNotNull() & F.col("order_id").isNotNull())
    .select(
        "*",
        F.to_date("paid_at").alias("paid_at_date"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .groupBy("status")
    .agg(
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.max("paid_at").alias("last_seen"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
    )
)
