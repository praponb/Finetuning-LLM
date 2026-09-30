from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel", "order_id").orderBy("paid_at", "id")
w6 = Window.partitionBy("channel").orderBy("session_id", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.orderBy("last_known_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("order_id")

enriched = (
    df_payments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.store_id").isNull())
    .select(
        "src.*",
        F.col("p.shipping_fee"),
        F.col("p.amount").alias("order_amount"),
        F.col("p.channel"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "amount", "paid_at", "order_amount", "channel")
)

with_prev = ranked.withColumn("prev_paid_at", F.lag("paid_at").over(w2))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "channel",
        F.sum("is_new_burst").over(w3).alias("session_id"),
    )
)

metrics = (
    islands
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.last("amount", ignorenulls=True).over(w4).alias("last_known_amount"),
        F.lead("session_id", 2).over(w5).alias("next_session_id"),
        F.avg("session_id").over(w6).alias("session_id_avg_2"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id").eqNullSafe(F.col("o.id")), "left")
    .select("a.id", "a.order_id", "a.last_known_amount", F.col("o.discount"))
)

result = (
    with_dims
    .select(
        "id",
        "order_id",
        "discount",
        F.first("last_known_amount", ignorenulls=True).over(w7).alias("first_known_last_known_amount"),
        F.sum("last_known_amount").over(w8).alias("cumulative_last_known_amount"),
        F.min("discount").over(w9).alias("order_id_min_discount"),
    )
)
