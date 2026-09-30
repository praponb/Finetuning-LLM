from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-02-08").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)

joined = (
    filtered
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.id", "s.store_id", F.col("d.sqft"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .filter(F.year(F.col("b.opened_date")) == 2024)
    .select(
        "src.id",
        "src.store_id",
        F.col("b.sqft").alias("store_sqft"),
        F.col("b.region"),
        F.col("b.city"),
    )
)

joined_inventory = (
    with_dims
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.region").alias("store_region"))
)

result = (
    joined_inventory
    .withColumn("store_sqft_tier", F.when(F.col("store_sqft") >= 16799, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["city", "store_sqft_tier", "store_id"], ["city", "store_sqft_tier"], []],
        "city",
        "store_sqft_tier",
        "store_id",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("store_region") == "North", F.col("store_sqft")).otherwise(0)).alias("north_store_sqft"),
        F.count_if(F.col("store_region") == "East").alias("east_events"),
        F.size(F.collect_set("store_region")).alias("num_store_regions"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
