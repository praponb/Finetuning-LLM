from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([1, 3, 5, 6]))
        .select("id", "store_id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = kept_rows.select("id", "store_id", "quantity", "last_restock")
