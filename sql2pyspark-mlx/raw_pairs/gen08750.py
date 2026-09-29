from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy("method", "status")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
    )
    .filter(F.col("refunded_rows") >= 1)
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
