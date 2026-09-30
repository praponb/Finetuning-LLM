from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("p.launch_date"),
        F.col("s.supplier_name"),
    )
)
