from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.product_id").isin([4, 5, 7, 8]))
    .groupBy(F.col("p.brand"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .filter(F.col("max_quantity") > 100)
)
