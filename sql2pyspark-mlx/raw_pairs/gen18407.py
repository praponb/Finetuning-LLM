from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("store_id") == 1)
    .select(
        "*",
        (F.datediff(F.current_date(), "last_restock") / 365).alias("years_since_last_restock"),
        F.dayofweek("last_restock").alias("last_restock_dayofweek"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.price"), F.col("d.product_name"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.region") == "East")
    .select("src.*", F.col("s.opened_date"), F.col("s.store_name"))
)

matched_keys = (
    enriched
    .select("store_id", "product_id")
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([1, 2, 5]))
        .select("store_id", "product_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-06-18")))
    .withColumn("quantity_whole", F.floor("quantity"))
)

matched_keys_2 = (
    base
    .select("store_id", "product_id")
    .intersect(
        df_inventory
        .filter(F.col("quantity").between(119, 159))
        .select("store_id", "product_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched_inventory = (
    kept_rows
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.last_restock",
        "src.product_name",
        "src.store_name",
        "src.quantity_whole",
        F.col("b.category"),
        F.col("b.price").alias("product_price"),
        F.col("b.product_name").alias("product_product_name"),
    )
)

summary = (
    enriched_inventory
    .groupBy(
        F.when(F.col("quantity_whole") >= 63, "HIGH").otherwise("LOW").alias("quantity_whole_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_rows"),
        F.max("quantity_whole").alias("max_quantity_whole"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.sort_array(F.collect_set("category")).alias("category_list_2"),
    )
)

result = (
    summary
    .select(
        "quantity_whole_tier",
        "distinct_ids",
        "electronics_rows",
        "category_list",
        "num_categories",
    )
    .orderBy("distinct_ids")
)
