from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.product_id").isin([1, 2, 4, 9]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("p.category"),
        F.col("p.cost"),
        F.col("s.store_name"),
        F.coalesce(F.col("s.store_name"), F.lit("unassigned")).alias("store_name_filled"),
    )
    .orderBy(F.col("store_name").desc(), "last_restock")
)
