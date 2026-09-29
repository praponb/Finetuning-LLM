from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"), F.col("s.country"))
    .agg(
        F.min(F.col("i.quantity")).alias("min_quantity"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
)
