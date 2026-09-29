from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.supplier_id").isin([11, 19, 25, 39]))
    .select(F.col("i.quantity"), F.col("p.launch_date"), F.col("s.city"))
)
