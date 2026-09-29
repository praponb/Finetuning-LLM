from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(-6, -1)
w_recent = Window.partitionBy("order_id").orderBy("unit_price", "id")
w4 = Window.partitionBy("channel").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("channel").orderBy("cumulative_shipping_fee", "id")

with_dims = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.unit_price",
        F.col("p.channel"),
        F.col("p.status"),
        F.col("p.shipping_fee"),
    )
)

enriched = (
    with_dims
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.stock_qty"), F.col("p.category"), F.col("p.cost"))
)

scored = (
    enriched
    .select(
        "*",
        F.last("status", ignorenulls=True).over(w).alias("last_known_status"),
        F.avg("shipping_fee").over(w2).alias("shipping_fee_avg_6_prior"),
        F.ntile(5).over(w_recent).alias("unit_price_bucket"),
        F.sum("shipping_fee").over(w4).alias("cumulative_shipping_fee"),
    )
)

enriched_order_items = (
    scored
    .filter(F.col("unit_price") > 200)
    .select(
        "id",
        "order_id",
        "unit_price",
        "channel",
        "shipping_fee",
        "stock_qty",
        "cost",
        "last_known_status",
        "unit_price_bucket",
        F.lag("cumulative_shipping_fee").over(w_part).alias("prev_cumulative_shipping_fee"),
        F.lead("cumulative_shipping_fee").over(w_part).alias("next_cumulative_shipping_fee"),
        F.lead("unit_price").over(w_part).alias("next_unit_price"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

result = (
    enriched_order_items
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["order_channel", "order_item_channel"], ["order_channel"]],
        F.col("p.channel").alias("order_channel"),
        F.col("e.channel").alias("order_item_channel"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.row_seq")).alias("min_row_seq"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
