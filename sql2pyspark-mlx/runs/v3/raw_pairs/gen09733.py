from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.product_id") == 4)
    .select(
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("i.store_id"),
    )
    .orderBy(F.col("supplier_id").desc())
)
