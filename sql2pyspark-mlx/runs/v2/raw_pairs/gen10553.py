from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isin(["London", "Tokyo"]))
    .groupBy(F.col("s.country"))
    .agg(
        F.count("*").alias("inventory_count"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
    .orderBy("country")
)
