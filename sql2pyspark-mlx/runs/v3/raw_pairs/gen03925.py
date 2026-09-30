from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("last_restock") < "2025-08-28").alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
    )
    .orderBy("id")
)
