from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .select(
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("p.supplier_id"),
        F.col("s.opened_date"),
    )
)
