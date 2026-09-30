from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.region") != "South") & (F.col("i.quantity") >= 210))
    .select(
        F.col("i.product_id"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
    .orderBy("opened_date")
)
