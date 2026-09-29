from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country") == "US")
    .groupBy(F.col("p.category"))
    .agg(
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy("category")
)
