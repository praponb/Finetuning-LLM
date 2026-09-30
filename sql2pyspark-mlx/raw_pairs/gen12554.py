from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-06-03 19:00:00").cast("timestamp")) & F.col("order_id").isin([2, 8, 12, 36]))
    .select("*", F.coalesce("method", F.lit("Unknown")).alias("method_filled"))
)

gaps = (
    base
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "method_filled",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .withColumn("session_id_tier", F.when(F.col("session_id") >= 7, "LARGE").otherwise("SMALL"))
    .rollup("session_id_tier")
    .agg(
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.max(F.when(F.col("method") == "paypal", F.col("session_id"))).alias("paypal_max_session_id"),
        F.percentile_approx("session_id", 0.95).alias("session_id_p95"),
        F.percentile_approx("session_id", 0.95).alias("session_id_p95_2"),
        F.grouping("session_id_tier").alias("grouping_level"),
    )
    .filter(F.col("refunded_events") < 1000)
    .orderBy(F.col("session_id_tier").desc())
)
