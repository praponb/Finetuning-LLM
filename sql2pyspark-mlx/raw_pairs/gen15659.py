from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("channel").orderBy("payment_paid_at", "id")
w4 = Window.partitionBy("channel").orderBy("rank_in_group", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("unit_price", "id")
w6 = Window.partitionBy("channel").orderBy(F.col("unit_price").asc_nulls_last())

base = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-09-17").cast("date"))
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
        F.concat_ws("-", "status", "channel").alias("status_channel_key"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled_2"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.channel_filled",
        "s.channel_filled_2",
        "s.order_item_quantity",
        "s.unit_price",
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)

latest = (
    with_latest
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "channel",
        "discount",
        "channel_filled",
        "unit_price",
        "rank_in_group",
        F.avg("discount").over(w4).alias("discount_avg_3"),
        F.lag("amount", 1, 0).over(w5).alias("prev_amount"),
        F.dense_rank().over(w6).alias("unit_price_dense_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("unit_price_dense_rank") >= 6, "LARGE").otherwise("SMALL").alias("unit_price_dense_rank_tier"),
    )
    .agg(
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events_2"),
        F.max(F.when(F.col("channel") == "store", F.col("unit_price_dense_rank"))).alias("store_max_unit_price_dense_rank"),
    )
)
