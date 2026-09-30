from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.stock_qty").between(136, 206))
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
)
