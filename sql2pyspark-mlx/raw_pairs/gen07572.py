from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("p.stock_qty") >= 256)
    .groupBy(F.col("p.brand"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
    .orderBy("brand")
)
