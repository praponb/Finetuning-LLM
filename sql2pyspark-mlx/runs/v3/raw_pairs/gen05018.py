from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter((F.col("i.quantity") <= 216) & F.col("i.store_id").isin([1, 3, 5]))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("s.opened_date"),
    )
)
