from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.product_id").isin([1, 2]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("id").desc())
)
