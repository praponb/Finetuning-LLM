from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter(F.col("i.product_id") == 7)
    .groupBy(F.col("p.category"), F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_products"))
)
