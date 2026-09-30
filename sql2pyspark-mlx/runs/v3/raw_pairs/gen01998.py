from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.store_id").isin([3, 5]) & F.col("s.store_name").like("Ai%"))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("p.brand"),
    )
    .orderBy(F.col("brand").desc(), "id")
    .limit(50)
)
