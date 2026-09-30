from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_last())
w2 = Window.partitionBy(F.month("last_restock")).orderBy(F.col("quantity").desc())

latest = (
    df_inventory
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    latest
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date") <= "2024-08-01")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.quantity",
        "a.last_restock",
        "a.dedup_rank",
        F.col("s.region"),
    )
)

matched_keys = (
    joined
    .select("id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") <= "2024-11-01")
        .select("id", "store_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "store_id",
        "quantity",
        "last_restock",
        "dedup_rank",
        "region",
        F.cume_dist().over(w2).alias("quantity_cume_dist"),
    )
)

joined_inventory = (
    enriched
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .select(
        "a.store_id",
        "a.region",
        F.col("p.city"),
        F.col("p.region").alias("store_region"),
        F.col("p.opened_date"),
    )
)

valid = (
    joined_inventory
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "region", "store_region").alias("region_store_region_key"),
        F.coalesce("region", F.lit("unassigned")).alias("region_filled"),
    )
)

totals = (
    valid
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
        F.countDistinct("city").alias("distinct_cities"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
        F.count(F.when(F.col("store_region") == "South", 1)).alias("south_count"),
    )
    .filter(F.col("west_rows") > 1)
)

result = (
    totals
    .select("quarter_start", "west_rows", "south_count")
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
