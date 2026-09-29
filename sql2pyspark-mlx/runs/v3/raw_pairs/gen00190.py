from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(F.count(F.col("i.quantity")).alias("quantity_count"))
)
