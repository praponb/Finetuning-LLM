from pyspark.sql import functions as F

scoped_skus = (
    df_skus
    .filter(F.length(F.trim(F.col("sku_code"))) >= 7)
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.array_join(F.array_sort(F.col("labels")), ",").alias("labels_text"),
        F.coalesce(F.col("unit_cost"), F.lit(0.0)).alias("unit_cost_filled"),
        F.abs(F.col("weight_kg") - 6.5).alias("weight_kg_distance"),
    )
)

enriched = (
    scoped_skus
    .select(
        "*",
        F.greatest(F.col("unit_cost"), F.col("weight_kg")).alias("greatest_unit_cost_weight_kg"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left_semi")
)

result = (
    without_matches
    .cube("category", "is_active")
    .agg(
        F.min("weight_kg_distance").alias("min_weight_kg_distance"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("unit_cost")).otherwise(0)).alias("unit_cost_category_electronics"),
    )
    .select(
        F.coalesce(F.col("category"), F.lit("ALL")).alias("category"),
        "is_active",
        "min_weight_kg_distance",
        "unit_cost_category_electronics",
    )
    .orderBy(F.col("category").asc_nulls_last(), F.col("is_active").asc_nulls_last())
)
