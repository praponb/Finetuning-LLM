from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter((F.size(F.col("p.tags")) > 2) & (F.col("p.stock_qty") < 167))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("p.price"),
        F.col("p.supplier_id"),
    )
    .orderBy(F.col("supplier_id").desc(), "id")
    .limit(5)
)
