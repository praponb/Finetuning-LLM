from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.length(F.col("s.store_name")) > 12)
    .groupBy(
        F.col("p.brand"),
        F.quarter(F.col("p.launch_date")).alias("launch_quarter"),
    )
    .agg(F.max(F.col("i.last_restock")).alias("last_last_restock"))
)
