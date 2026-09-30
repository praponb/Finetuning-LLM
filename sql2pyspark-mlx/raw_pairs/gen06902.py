from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.quantity"),
        F.col("p.supplier_id"),
        F.col("s.city"),
        (F.col("p.stock_qty") - F.col("p.cost")).alias("stock_qty_minus_cost"),
    )
)
