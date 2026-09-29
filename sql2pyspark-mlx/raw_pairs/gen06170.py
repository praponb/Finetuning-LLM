from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.launch_date").between("2024-03-01", "2024-07-22"))
    .select(F.col("i.store_id"), F.col("p.supplier_id"))
    .orderBy(F.col("supplier_id").desc())
)
