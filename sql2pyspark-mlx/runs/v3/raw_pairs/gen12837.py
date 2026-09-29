from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("i.quantity"),
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("s.rating"),
        F.lower(F.col("s.city")).alias("city_lower"),
    )
)
