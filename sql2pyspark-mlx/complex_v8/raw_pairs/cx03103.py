from pyspark.sql import functions as F

base = (
    df_skus
    .filter((F.col("is_active")) & (F.col("unit_cost") <= 320.4))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

category_stats = (
    base
    .groupBy("category")
    .agg(F.avg("weight_kg").alias("avg_weight_kg"))
)

vs_group = (
    base.alias("s")
    .join(category_stats.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.weight_kg") > F.col("g.avg_weight_kg"))
    .select(
        "s.*",
        "g.avg_weight_kg",
        F.round(F.col("s.weight_kg") - F.col("g.avg_weight_kg"), 2).alias("weight_kg_minus_avg"),
    )
)

category_stats_2 = (
    vs_group
    .groupBy("category")
    .agg(F.avg("weight_kg").alias("avg_weight_kg_2"))
)

outliers = (
    vs_group.alias("s")
    .join(category_stats_2.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.weight_kg") > F.col("g.avg_weight_kg_2"))
    .select(
        "s.*",
        "g.avg_weight_kg_2",
        F.round(F.col("s.weight_kg") - F.col("g.avg_weight_kg_2"), 2).alias("weight_kg_minus_avg_2"),
    )
)

result = (
    outliers
    .groupingSets(
        [["category", "is_active"], ["category"], ["is_active"]],
        "category", "is_active",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
        F.round(F.sum("weight_kg_minus_avg_2") / F.nullif(F.sum("unit_cost"), F.lit(0)), 3).alias("weight_kg_minus_avg_2_to_unit_cost_ratio"),
    )
    .orderBy(F.col("category").asc_nulls_last(), F.col("is_active").asc_nulls_last())
)
