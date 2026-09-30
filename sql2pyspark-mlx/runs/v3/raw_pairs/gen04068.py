from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.month(F.col("s.opened_date")).isin([1, 11, 12]))
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
)
