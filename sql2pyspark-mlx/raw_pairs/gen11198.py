from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
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
    sessionized
    .groupBy(
        F.when(F.col("burst_id") >= 7, "HIGH").otherwise("LOW").alias("burst_id_tier"),
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min_2"),
    )
)
