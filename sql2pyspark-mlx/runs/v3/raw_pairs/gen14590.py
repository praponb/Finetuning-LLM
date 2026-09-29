from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter((F.col("i.quantity") == 123) & (F.col("i.quantity") < 181))
    .select(
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("product_id").desc())
)
