from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-06-09").cast("date")) & F.col("product_id").isin([7, 8, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

remaining_keys = (
    valid
    .select("product_id", "store_id")
    .exceptAll(
        df_inventory
        .filter(F.col("last_restock") > "2024-04-07")
        .select("product_id", "store_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("quantity_whole").between(31, 73))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

enriched = (
    cleaned
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.region"), F.col("s.store_name"), F.col("s.city"))
)

filtered = (
    enriched
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "quantity_filled",
        "region",
        "store_name",
        "city",
        F.round("quantity_filled", 2).alias("quantity_filled_rounded"),
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

result = (
    filtered
    .cube("store_id")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.sort_array(F.collect_set("city")).alias("city_list_2"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
    )
)
