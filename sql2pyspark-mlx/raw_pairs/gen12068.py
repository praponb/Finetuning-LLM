from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipping_fee").desc_nulls_last())
w2 = Window.partitionBy("status", "order_id")
w3 = Window.partitionBy("order_id").orderBy(F.col("shipping_fee").asc_nulls_last())

matched_keys = (
    df_shipments
    .select("id", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") >= "2025-03-30")
        .select("id", "order_id")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "left")
    .filter(F.col("o.amount") <= 480)
    .select(
        "src.id",
        "src.order_id",
        "src.shipped_date",
        "src.delivered_date",
        "src.status",
        F.col("o.shipping_fee"),
    )
)

enriched_shipments = (
    enriched
    .select(
        "id",
        "order_id",
        "delivered_date",
        "shipping_fee",
        F.percent_rank().over(w).alias("shipping_fee_percent_rank"),
        F.stddev_samp("shipping_fee").over(w2).alias("status_stddev_samp_shipping_fee"),
        F.cume_dist().over(w3).alias("shipping_fee_cume_dist"),
        F.max("shipping_fee").over(w2).alias("status_max_shipping_fee"),
        F.sum("shipping_fee").over(w2).alias("status_sum_shipping_fee"),
    )
)

with_dims = (
    enriched_shipments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.delivered_date",
        "src.status_max_shipping_fee",
        F.col("p.status"),
        F.col("p.shipping_fee").alias("order_shipping_fee"),
    )
)

result = (
    with_dims
    .select("id", "status_max_shipping_fee", "status", "order_shipping_fee")
    .orderBy(F.col("status_max_shipping_fee").desc_nulls_last())
)
