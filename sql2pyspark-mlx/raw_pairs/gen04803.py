from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.col("p.price") > 710) & (F.col("s.city") == "Tokyo"))
    .select(F.col("i.last_restock"), F.col("p.stock_qty"), F.col("s.opened_date"))
)
