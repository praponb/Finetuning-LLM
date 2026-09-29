from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.status"), F.col("p.amount"))
)

filtered_order_items = (
    enriched
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        F.least("unit_price", "line_total").alias("least_unit_price_line_total"),
        (F.col("quantity") + F.col("line_total")).alias("quantity_plus_line_total"),
    )
)

result = (
    filtered_order_items
    .select(
        "id",
        "quantity",
        "unit_price",
        "line_total",
        "amount",
        "least_unit_price_line_total",
    )
)
