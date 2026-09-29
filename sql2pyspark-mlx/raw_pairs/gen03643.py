from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.product_id") == 6)
    .select(
        F.col("p.category"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("i.quantity"),
    )
    .orderBy(F.col("quantity").desc())
)
