from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.quantity") >= 163)
    .groupBy(F.col("p.brand"))
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
    .orderBy("quantity_range", "brand")
    .limit(20)
)
