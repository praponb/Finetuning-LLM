from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-08-27 20:00:00").cast("timestamp"))
    .withColumn("amount_scaled", F.round((F.col("amount") * 1.1), 2))
    .withColumn("method_clean", F.coalesce("method", F.lit("UNKNOWN")))
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "method_clean",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .cube("activity_month", "method")
    .agg(
        F.avg("session_id").alias("avg_session_id"),
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("cash_events") >= 1)
    .orderBy("method")
)
