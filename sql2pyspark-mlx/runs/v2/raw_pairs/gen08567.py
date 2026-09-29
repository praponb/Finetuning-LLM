from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region").isin(["Central", "West", "East"]))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("p.supplier_id"),
        F.col("s.opened_date"),
        F.col("s.city"),
    )
    .orderBy("quantity")
)
