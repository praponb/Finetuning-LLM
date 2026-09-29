from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter((F.col("s.sqft") >= 12754) & (F.col("s.city") == "New York"))
    .select(
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
    )
    .orderBy("store_name")
)
