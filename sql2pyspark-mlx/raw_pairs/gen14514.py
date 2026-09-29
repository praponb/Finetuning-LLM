from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("prev_paid_at", "id")
w6 = Window.partitionBy("order_id").orderBy("prev_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("paid_at") > F.lit("2023-09-07").cast("date")))
    .withColumn("method_group", F.when(F.col("method").isin(["bank_transfer", "card"]), "Group A").otherwise("Other"))
    .withColumn("adjusted_amount", F.round((F.col("amount") * 0.9), 2))
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

with_prev = islands.withColumn("prev_paid_at", F.lag("paid_at").over(w3))

with_gaps_2 = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps_2.withColumn("session_no", F.sum("new_group_flag").over(w4))

flagged = (
    sessions_payments
    .withColumn("is_new_burst", F.when(F.lag("prev_paid_at").over(w5).isNull() | (F.col("prev_paid_at") > F.lag("prev_paid_at").over(w5) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_payments_2 = flagged.withColumn("session_no_2", F.sum("is_new_burst").over(w6))

with_dims = (
    sessions_payments_2
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.prev_paid_at",
        "src.new_group_flag",
        "src.session_no",
        F.col("d.discount"),
        F.col("d.status"),
    )
)

result = (
    with_dims
    .alias("s")
    .join(df_orders.alias("dim"), F.col("s.order_id") == F.col("dim.id"), "left")
    .groupBy(
        F.col("dim.status").alias("order_status"),
        F.col("dim.channel").alias("channel"),
        F.col("s.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.session_no")), 2).alias("total_session_no"),
    )
)
