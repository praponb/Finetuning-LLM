from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.size(F.col("p.tags")) > 2)
    .groupBy(F.month(F.col("p.launch_date")).alias("launch_month"), F.col("p.category"))
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
    .orderBy(F.col("category").desc())
)
