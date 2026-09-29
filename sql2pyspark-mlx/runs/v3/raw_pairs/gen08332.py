from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date").between("2024-05-01", "2024-11-01"))
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.min(F.col("i.last_restock")).alias("first_last_restock"))
    .orderBy(F.col("first_last_restock").desc())
)
