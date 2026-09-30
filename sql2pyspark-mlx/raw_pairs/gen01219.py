from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("s.city").isin(["Tokyo", "London"]) & F.col("i.product_id").isin([1, 6]))
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
    .orderBy("opened_date")
)
