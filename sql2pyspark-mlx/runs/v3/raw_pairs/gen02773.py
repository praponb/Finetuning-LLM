from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.product_id").isin([4, 6, 10]) & F.col("s.sqft").between(8447, 14034))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
        F.col("s.region"),
        F.col("s.opened_date"),
    )
)
