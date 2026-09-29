from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("s.city").isin(["New York", "Chiang Mai", "Bangkok"]) & F.col("p.category").isin(["Sports", "Books", "Home"]))
    .select(
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("p.category"),
        F.col("p.supplier_id"),
    )
)
