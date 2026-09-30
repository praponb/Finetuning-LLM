from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.product_id").isin([2, 7, 8, 9]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
    )
)
