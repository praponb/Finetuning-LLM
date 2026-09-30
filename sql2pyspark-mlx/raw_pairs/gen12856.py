from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("order_id").isin([9, 22, 34, 40]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.store_id").isin([4, 23]))
    .select(
        "a.id",
        "a.order_id",
        "a.line_total",
        F.col("b.order_date"),
        F.col("b.channel"),
        F.col("b.amount"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.status"),
        F.col("b.order_date").alias("order_order_date"),
        F.col("b.shipping_fee"),
    )
)

with_dims_order_items = (
    with_dims
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.channel").alias("order_channel"))
)

result = (
    with_dims_order_items
    .groupBy("status")
    .pivot("order_channel", ["web", "mobile", "partner"])
    .agg(F.max("amount").alias("total"))
)
