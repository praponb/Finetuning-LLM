from pyspark.sql import functions as F

cleaned = (
    df_skus
    .filter((F.col("weight_kg") > 8.5) | (~F.array_contains(F.col("labels"), "fragile")))
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        F.array_contains(F.col("labels"), "fragile").alias("has_fragile"),
        F.concat_ws("-", F.col("sku_code"), F.col("category")).alias("sku_code_category_label"),
        F.concat(F.col("sku_code"), F.lit("#"), F.col("id").cast("string")).alias("sku_code_ref"),
    )
)

filtered = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.unit_price") < 292.3), "left_anti")
)

filtered_2 = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.warehouse_id") >= 2), "left_anti")
)

stats = (
    filtered_2
    .groupBy("category", "has_fragile")
    .agg(
        F.array_sort(F.collect_set("category")).alias("category_list"),
        F.countDistinct("category").alias("distinct_categorys"),
        F.round(F.sum("weight_kg"), 2).alias("total_weight_kg"),
        F.round(100 * F.count(F.when(F.col("category").isNotNull(), 1)) / F.count("*"), 1).alias("pct_category_is_not_null"),
    )
)

with_activity = (
    stats
    .crossJoin(stats.agg(F.min("pct_category_is_not_null").alias("overall_min_pct_category_is_not_null")))
    .filter(F.col("pct_category_is_not_null") > F.col("overall_min_pct_category_is_not_null"))
    .drop("overall_min_pct_category_is_not_null")
)

qualified = (
    with_activity
    .crossJoin(with_activity.agg(F.max("distinct_categorys").alias("overall_max_distinct_categorys")))
    .filter(F.col("distinct_categorys") < F.col("overall_max_distinct_categorys"))
    .drop("overall_max_distinct_categorys")
)

enriched = (
    qualified
    .select(
        "category",
        "has_fragile",
        "category_list",
        "total_weight_kg",
        "pct_category_is_not_null",
        F.when(F.col("pct_category_is_not_null") >= 85.0, "large").when(F.col("pct_category_is_not_null") >= 45.0, "medium").otherwise("small").alias("pct_category_is_not_null_tier"),
        F.when(F.col("total_weight_kg") >= 112.5, "A").when(F.col("total_weight_kg") >= 52.6, "B").otherwise("C").alias("total_weight_kg_tier"),
    )
)

result = (
    enriched
    .select(
        "category",
        "category_list",
        "total_weight_kg",
        "pct_category_is_not_null",
        "pct_category_is_not_null_tier",
        "total_weight_kg_tier",
        F.lower(F.regexp_replace(F.trim(F.col("total_weight_kg_tier")), " ", "_")).alias("total_weight_kg_tier_slug"),
        F.size(F.col("category_list")).alias("category_list_count"),
    )
)
