from pyspark.sql import functions as F

base_skus = (
    df_skus
    .filter((F.col("weight_kg") > 6.4) & (F.col("sku_code").isNotNull()))
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "is_active",
        F.concat_ws(" / ", F.col("sku_code"), F.col("category")).alias("sku_code_category_label"),
    )
)

tagged = (
    base_skus
    .filter(F.col("sku_code_category_label").isNotNull())
    .select(
        "*",
        F.when(F.col("category").isin("Food", "Apparel"), "group_a").when(F.col("category") == "Electronics", "group_b").otherwise("other").alias("category_group"),
        F.substring(F.trim(F.col("category")), 1, 3).alias("category_prefix"),
    )
)

category_group_stats = (
    tagged
    .groupBy("category_group")
    .agg(F.avg("weight_kg").alias("avg_weight_kg"))
)

vs_group = (
    tagged.alias("s")
    .join(category_group_stats.alias("g"), F.col("g.category_group") == F.col("s.category_group"), "inner")
    .filter(F.col("s.weight_kg") > F.col("g.avg_weight_kg"))
    .select(
        "s.*",
        "g.avg_weight_kg",
        F.round(F.col("s.weight_kg") - F.col("g.avg_weight_kg"), 2).alias("weight_kg_minus_avg"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.sum("qty").alias("total_move_qty"),
        F.count("*").alias("move_count_2"),
    )
)

summarized_rows = (
    vs_group.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.total_move_qty",
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

result = (
    summarized_rows
    .cube("is_active", "move_count")
    .agg(
        F.grouping("is_active").alias("is_total_is_active"),
        F.array_sort(F.collect_set("category")).alias("category_list"),
        F.round(F.sum("total_move_qty") / F.nullif(F.sum("move_count_2"), F.lit(0)), 3).alias("total_move_qty_to_move_count_2_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
