from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("s.opened_date") > "2023-05-01")
    .select(
        F.col("s.store_name"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.date_add(F.col("i.last_restock"), 80).alias("last_restock_plus_80d"),
    )
)
