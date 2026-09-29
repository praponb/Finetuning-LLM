from pyspark.sql import functions as F

valid_skus = (
    df_skus
    .filter((F.col("weight_kg") > 4.7) & (F.col("unit_cost").between(60.9, 330.6)))
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "labels",
        "is_active",
        F.greatest(F.col("weight_kg"), F.col("unit_cost")).alias("greatest_weight_kg_unit_cost"),
        F.length(F.trim(F.col("category"))).alias("category_len"),
        F.round(F.col("unit_cost") * 1.07, 2).alias("unit_cost_adj"),
    )
)

category_stats = (
    valid_skus
    .groupBy("category")
    .agg(F.max("unit_cost_adj").alias("max_unit_cost_adj"))
)

outliers = (
    valid_skus.alias("s")
    .join(category_stats.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.unit_cost_adj") == F.col("g.max_unit_cost_adj"))
    .select(
        "s.*",
        "g.max_unit_cost_adj",
        F.round(F.col("s.unit_cost_adj") - F.col("g.max_unit_cost_adj"), 2).alias("unit_cost_adj_minus_max"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count(F.when(F.col("operator").isin("op_ann", "op_bee"), 1)).alias("move_operator_count"),
    )
)

with_counts = (
    outliers.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

category_stats_2 = (
    with_counts
    .groupBy("category")
    .agg(F.max("weight_kg").alias("max_weight_kg"))
)

outliers_2 = (
    with_counts.alias("s")
    .join(category_stats_2.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.weight_kg") == F.col("g.max_weight_kg"))
    .select(
        "s.*",
        "g.max_weight_kg",
        F.round(F.col("s.weight_kg") - F.col("g.max_weight_kg"), 2).alias("weight_kg_minus_max"),
    )
)

result = (
    outliers_2
    .groupingSets(
        [["is_active", "category_len"], ["is_active"], ["category_len"], []],
        "is_active", "category_len",
    )
    .agg(
        F.max("last_moved_at").alias("last_last_moved_at"),
        F.array_sort(F.collect_set("category")).alias("category_list"),
        F.round(F.sum("greatest_weight_kg_unit_cost") / F.nullif(F.sum("unit_cost_adj"), F.lit(0)), 3).alias("greatest_weight_kg_unit_cost_to_unit_cost_adj_ra"),
    )
)
