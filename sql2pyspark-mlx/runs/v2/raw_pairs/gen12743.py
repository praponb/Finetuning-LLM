from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "right")
    .filter(F.col("p.supplier_id").isNotNull() & (F.col("s.opened_date") <= "2025-06-01"))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("s.store_name"),
        F.concat_ws("-", F.col("s.region"), F.col("s.store_name")).alias("region_store_name_key"),
    )
    .orderBy(F.col("region_store_name_key").desc())
)
