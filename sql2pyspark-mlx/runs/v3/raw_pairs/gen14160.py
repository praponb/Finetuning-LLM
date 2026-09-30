from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.quantity") < 225)
    .select(
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        (F.col("p.cost") + F.col("p.stock_qty")).alias("cost_plus_stock_qty"),
    )
    .orderBy(F.col("store_id").desc())
)
