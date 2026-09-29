from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "order_id").orderBy(F.col("order_date").desc_nulls_first(), "id")
w_hist = Window.partitionBy("order_id").orderBy("amount", "id")

enriched = (
    df_payments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.amount",
        "s.status",
        F.col("p.amount").alias("order_amount"),
        F.col("p.shipping_fee"),
        F.col("p.order_date"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.amount",
        "e.order_amount",
        "e.shipping_fee",
        F.col("d.amount").alias("order_amount_2"),
        F.col("d.status").alias("order_status"),
        F.col("d.order_date").alias("order_order_date"),
    )
)

with_dims = (
    joined
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.*",
        F.col("b.channel"),
        F.col("b.order_date"),
        F.col("b.shipping_fee").alias("order_shipping_fee"),
    )
)

latest = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "order_id",
        "amount",
        "shipping_fee",
        "order_amount_2",
        "order_status",
        "order_date",
    )
)

metrics = (
    latest
    .select(
        "*",
        F.lag("amount").over(w_hist).alias("prev_amount"),
        F.lead("order_amount_2").over(w_hist).alias("next_order_amount_2"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "amount",
        "order_amount_2",
        "order_status",
        "order_date",
        "prev_amount",
        "next_order_amount_2",
        "row_seq",
    )
    .orderBy(F.col("order_date").desc_nulls_first())
)
