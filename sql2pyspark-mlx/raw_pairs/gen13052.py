from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id")
w4 = Window.orderBy(F.col("cumulative_sqft_share").desc())

with_dims = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .filter(F.col("d.city") == "Singapore")
    .select("e.id", "e.store_id", "e.last_restock", F.col("d.city"), F.col("d.sqft"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "store_id",
        "last_restock",
        F.max("sqft").over(w).alias("running_max_sqft"),
        F.count("sqft").over(w).alias("running_count_sqft"),
        F.min("sqft").over(w).alias("running_min_sqft"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
    )
)

remaining_keys = (
    with_windows
    .select("store_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([1, 3]))
        .select("store_id", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows_inventory = (
    kept_rows
    .select(
        "id",
        "last_restock",
        "running_max_sqft",
        F.round((F.col("running_count_sqft") / F.sum("running_count_sqft").over(w3)), 4).alias("running_count_sqft_share"),
        F.round((F.col("cumulative_sqft") / F.sum("cumulative_sqft").over(w3)), 4).alias("cumulative_sqft_share"),
        F.round((F.col("running_min_sqft") / F.sum("running_min_sqft").over(w3)), 4).alias("running_min_sqft_share"),
    )
)

kept_keys = (
    with_windows_inventory
    .select("id")
    .exceptAll(
        df_inventory
        .filter(F.col("product_id").isin([1, 7, 8]))
        .select("id")
    )
)

reconciled = (
    with_windows_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "running_max_sqft",
        "cumulative_sqft_share",
        F.rank().over(w4).alias("cumulative_sqft_share_rank"),
    )
)
