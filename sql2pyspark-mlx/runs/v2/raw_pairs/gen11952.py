from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.store_id").isin([2, 3]))
    .groupBy(F.col("p.category"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .filter(F.col("max_quantity") >= 80)
    .orderBy("category")
)
