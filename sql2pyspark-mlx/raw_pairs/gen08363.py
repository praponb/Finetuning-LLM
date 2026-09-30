from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("region").orderBy("sqft", "id")

filtered = (
    df_inventory
    .filter(F.col("product_id").isin([2, 4, 6, 7]))
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

with_dims = (
    filtered
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.quantity",
        "e.last_restock",
        "e.quantity_filled",
        F.col("b.sqft"),
        F.col("b.region"),
        F.col("b.store_name"),
    )
)

with_windows = (
    with_dims
    .select(
        "*",
        F.lag("sqft").over(w_running).alias("prev_sqft"),
        F.lag("quantity_filled").over(w_running).alias("prev_quantity_filled"),
        F.lead("quantity_filled").over(w_running).alias("next_quantity_filled"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

kept_keys = (
    with_windows
    .select("store_id", "id")
    .intersect(
        df_inventory
        .filter(F.col("product_id").isin([1, 6, 8]))
        .select("store_id", "id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    verified
    .select("store_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("last_restock") >= "2023-04-01")
        .select("store_id", "id")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("quantity") >= 266, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        "store_id",
    )
    .agg(
        F.count(F.when(F.col("region") == "South", 1)).alias("south_rows"),
        F.percentile_approx("quantity_filled", 0.95).alias("quantity_filled_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity_filled", [0.25, 0.5, 0.75]).alias("quantity_filled_quartiles"),
        F.sum(F.when(F.col("region") == "South", F.col("seq_no")).otherwise(0)).alias("south_seq_no"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
    )
    .filter(F.col("south_seq_no") > 0)
    .orderBy("west_rows")
)
