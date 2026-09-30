from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(~(F.col("i.store_id").isin([3, 5]) & (F.col("s.region") == "East")))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.date_trunc("week", F.col("i.last_restock")).alias("last_restock_period"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(5)
)
