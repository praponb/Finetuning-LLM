from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("cost").isNotNull()).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("i.last_restock").between("2024-01-01", "2024-04-01"))
    .select(F.col("i.id"), F.col("i.quantity"), F.col("i.product_id"))
)
