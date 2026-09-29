from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.last_restock") < "2025-01-02")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("p.launch_date"),
        F.initcap(F.trim(F.col("p.category"))).alias("category_title"),
    )
    .orderBy("last_restock")
)
