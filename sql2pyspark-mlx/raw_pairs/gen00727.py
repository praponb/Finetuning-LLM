from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.quantity") != 179)
    .select(F.col("i.id"), F.col("i.store_id"), F.col("p.cost"), F.col("s.opened_date"))
)
