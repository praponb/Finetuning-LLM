from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.last_restock"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
)
