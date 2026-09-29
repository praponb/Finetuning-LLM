from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.order_date") > "2023-12-01") & F.col("o.store_id").isin([2, 4, 31]))
    .select(
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("p.supplier_id"),
        F.col("o.channel"),
    )
    .orderBy("channel")
)
