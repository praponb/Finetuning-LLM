from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("shipped_date")
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")

enriched = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.carrier",
        "e.shipped_date",
        "e.delivered_date",
        "e.cost",
        F.col("p.order_date"),
        F.col("p.shipping_fee"),
        F.col("p.amount"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_dims = (
    deduped
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.order_date").between("2024-12-25", "2025-10-01"))
    .select(
        "s.id",
        "s.order_id",
        "s.shipped_date",
        "s.cost",
        F.col("b.shipping_fee").alias("order_shipping_fee"),
        F.col("b.amount").alias("order_amount"),
    )
)

latest = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    latest
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.shipped_date",
        "e.cost",
        "e.order_amount",
        "e.dedup_rank",
        F.col("d.shipping_fee"),
        F.col("d.discount"),
        F.col("d.order_date"),
    )
)

result = (
    joined
    .select("id", "shipped_date", "order_amount", "shipping_fee", "discount")
    .orderBy(F.col("id").desc(), "discount")
    .limit(5)
)
