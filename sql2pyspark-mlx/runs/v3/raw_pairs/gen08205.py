from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("s.region").isNotNull() & F.col("s.region").isin(["North", "Central", "West"]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
)
