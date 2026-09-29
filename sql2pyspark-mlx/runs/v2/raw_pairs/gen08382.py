from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id") == 23)
    .groupBy(F.col("p.category"), F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(
        F.min(F.col("i.quantity")).alias("min_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .orderBy("min_quantity", F.col("quantity_count").desc())
)
