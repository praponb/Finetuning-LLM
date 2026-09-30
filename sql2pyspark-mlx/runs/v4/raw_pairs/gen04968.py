from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id", "method").orderBy("order_amount", "id")
w2 = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("order_amount", "id")
w6 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("order_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("status", "channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("channel").orderBy("paid_at", "id")
w11 = Window.partitionBy("channel").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_payments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.channel"),
        F.col("b.amount").alias("order_amount"),
        F.col("b.shipping_fee"),
    )
)

enriched_payments = (
    enriched
    .withColumn("order_amount_bucket", F.ntile(3).over(w_grp))
    .withColumn("running_order_amount", F.sum("order_amount").over(w2))
)

gaps = (
    enriched_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_session").over(w4))

scored = (
    sessionized
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("order_amount_bucket_avg_6_prior", F.avg("order_amount_bucket").over(w6))
    .withColumn("last_known_running_order_amount", F.last("running_order_amount", ignorenulls=True).over(w7))
    .withColumn("running_order_amount_avg_3", F.avg("running_order_amount").over(w_recent))
    .withColumn("first_known_order_amount", F.first("order_amount", ignorenulls=True).over(w9))
)

with_gaps = (
    scored
    .withColumn("is_new_session_2", F.when(F.lag("paid_at").over(w10).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w10) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_id", F.sum("is_new_session_2").over(w11))

summary = (
    sessions_payments
    .groupBy(F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("method").alias("distinct_methods"),
    )
)

result = (
    summary
    .select("quarter_start", "event_count", "distinct_methods")
    .orderBy(F.col("quarter_start").desc(), "distinct_methods")
    .limit(20)
)
