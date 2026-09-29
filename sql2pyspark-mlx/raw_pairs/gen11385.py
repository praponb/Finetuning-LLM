from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("quantity") == 87)
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)

result = (
    base
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.id", "s.store_id", "s.last_restock", F.col("b.region"))
)
