from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
    )
)
