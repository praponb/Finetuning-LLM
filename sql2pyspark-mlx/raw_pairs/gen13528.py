from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("method") == "cash") & F.col("method").isNotNull())
    .select("*", F.substring("status", 1, 4).alias("status_prefix"))
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy("burst_id", F.date_trunc("WEEK", "paid_at").alias("week_start"), "method")
    .agg(
        F.round(F.sum("is_new_session") / F.nullif(F.count(F.when(F.col("method") == "card", 1)), F.lit(0)), 2).alias("is_new_session_per_card"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
)
