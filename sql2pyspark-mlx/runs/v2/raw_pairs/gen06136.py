from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.product_id").isin([4, 6, 7, 9]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("s.city"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
