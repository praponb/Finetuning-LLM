from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("years_since_last_restock", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("years_since_last_restock").desc_nulls_last())

filtered = (
    df_inventory
    .filter(F.col("last_restock") > "2025-02-23")
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

filtered_inventory = (
    filtered
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .withColumn("quantity_plus_quantity_filled", (F.col("quantity") + F.col("quantity_filled")))
    .withColumn("years_since_last_restock", (F.datediff(F.current_date(), "last_restock") / 365))
)

with_windows = (
    filtered_inventory
    .filter(F.month("last_restock").isin([1, 5, 9]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity_filled",
        "quantity_plus_quantity_filled",
        F.avg("years_since_last_restock").over(w_hist).alias("years_since_last_restock_avg_6"),
        F.dense_rank().over(w2).alias("years_since_last_restock_dense_rank"),
    )
)

remaining_keys = (
    with_windows
    .select("store_id", "id")
    .intersect(
        df_inventory
        .filter(F.col("product_id").isin([4, 8]))
        .select("store_id", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    kept_rows
    .select("id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("product_id") == 5)
        .select("id", "store_id")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.region").alias("region"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("e.years_since_last_restock_dense_rank")), 2).alias("total_years_since_last_restock_dense_rank"),
        F.round(F.sum(F.col("e.quantity_plus_quantity_filled")), 2).alias("total_quantity_plus_quantity_filled"),
    )
)
