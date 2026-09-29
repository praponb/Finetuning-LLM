from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    gaps
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_payments
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .rollup("order_id", "year_start")
    .agg(
        F.max("paid_at").alias("last_seen"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id_2"),
        F.count("*").alias("cnt"),
        F.max("paid_at").alias("last_seen_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("cnt").desc())
)
