from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("p.supplier_id"),
        F.col("p.cost"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.trim(F.col("s.store_name")).alias("clean_store_name"),
    )
    .orderBy("supplier_id")
)
