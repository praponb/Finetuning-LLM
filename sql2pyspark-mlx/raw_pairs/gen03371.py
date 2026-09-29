from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .select(F.col("i.quantity"), F.col("i.last_restock"), F.col("p.supplier_id"))
    .orderBy(F.col("quantity").desc(), F.col("last_restock").desc())
)
