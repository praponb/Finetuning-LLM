from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("i.product_id").isin([7, 8, 10]))
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .orderBy("avg_quantity", "quantity_count", "supplier_id")
    .limit(10)
)
