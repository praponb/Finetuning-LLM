from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .select(
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
    .orderBy(F.col("product_id").desc())
)
