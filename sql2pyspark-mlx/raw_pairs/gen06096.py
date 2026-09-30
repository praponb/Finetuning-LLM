from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("quantity")
w2 = Window.partitionBy(F.date_trunc("YEAR", "last_restock"), "product_id")
w_running = Window.partitionBy("store_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())

matched_keys = (
    df_inventory
    .select("id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("last_restock") > "2024-01-01")
        .select("id", "store_id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.percent_rank().over(w).alias("quantity_percent_rank"),
        F.max("quantity").over(w2).alias("last_restock_period_max_quantity"),
        F.count("quantity").over(w_running).alias("store_id_count_quantity"),
        F.stddev_samp("quantity").over(w_running).alias("store_id_stddev_samp_quantity"),
        F.cume_dist().over(w4).alias("quantity_cume_dist"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .filter(F.col("p2.store_name").isNotNull())
    .select("s.*", F.col("p2.sqft"), F.col("p2.city"), F.col("p2.store_name"))
)

enriched_inventory = (
    with_dims
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .select(
        "a.*",
        F.col("b.store_name").alias("store_store_name"),
        F.col("b.opened_date"),
        F.col("b.sqft").alias("store_sqft"),
    )
)

enriched_inventory_2 = (
    enriched_inventory
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select(
        "a.*",
        F.col("s.opened_date").alias("store_opened_date"),
        F.col("s.sqft").alias("store_sqft_2"),
        F.col("s.city").alias("store_city"),
    )
)

result = (
    enriched_inventory_2
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.city").alias("store_city"), F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.store_sqft_2")), 2).alias("total_store_sqft_2"),
        F.grouping_id().alias("gid"),
    )
)
