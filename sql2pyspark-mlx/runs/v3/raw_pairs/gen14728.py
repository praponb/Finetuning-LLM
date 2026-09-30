from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("p.brand") == "Stark")
    .groupBy(F.col("p.id"), F.col("p.category"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy("latest_last_restock", "id", "category")
    .limit(50)
)
