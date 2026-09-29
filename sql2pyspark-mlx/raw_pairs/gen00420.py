from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter((F.col("s.opened_date") <= "2023-08-09") & (F.year(F.col("s.opened_date")) >= 2024))
    .select(
        F.col("s.sqft"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("i.product_id"),
    )
)
