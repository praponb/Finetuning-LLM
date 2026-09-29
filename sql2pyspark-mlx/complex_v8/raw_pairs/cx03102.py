from pyspark.sql import functions as F

filtered = (
    df_skus
    .select(
        "id",
        "category",
        "weight_kg",
        "labels",
        "is_active",
        F.initcap(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
        F.when(F.col("category").isin("Beauty", "Electronics"), "group_a").when(F.col("category") == "Toys", "group_b").otherwise("other").alias("category_group"),
        F.greatest(F.col("unit_cost"), F.col("weight_kg")).alias("greatest_unit_cost_weight_kg"),
    )
)

without_matches = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.moved_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp")), "left_anti")
)

screened = (
    without_matches
    .crossJoin(without_matches.agg(F.avg("greatest_unit_cost_weight_kg").alias("overall_avg_greatest_unit_cost_weight_kg")))
    .filter(F.col("greatest_unit_cost_weight_kg") > F.col("overall_avg_greatest_unit_cost_weight_kg"))
    .drop("overall_avg_greatest_unit_cost_weight_kg")
)

result = (
    screened
    .cube("category_group", "is_active", "category")
    .agg(
        F.grouping("category_group").alias("is_total_category_group"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("category").alias("distinct_categorys"),
    )
    .orderBy(
        F.col("category_group").asc_nulls_last(),
        F.col("is_active").asc_nulls_last(),
        F.col("category").asc_nulls_last(),
    )
)
