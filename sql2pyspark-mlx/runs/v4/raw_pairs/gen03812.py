from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
)
