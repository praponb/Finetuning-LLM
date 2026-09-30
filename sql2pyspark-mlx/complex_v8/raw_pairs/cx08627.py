from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category = Window.partitionBy("category")
w_all_by_unit_cost = Window.orderBy(F.col("unit_cost").desc(), "id")
w_all_by_unit_cost_ties = Window.orderBy("unit_cost")
w_all_by_unit_cost_prior_6 = Window.orderBy(F.col("unit_cost").desc(), "id").rowsBetween(-6, -1)

base = (
    df_skus
    .filter((F.col("is_active")) & (~F.array_contains(F.col("labels"), "fragile")))
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.when(F.col("weight_kg") >= 20.8, "A").when(F.col("weight_kg") >= 6.5, "B").otherwise("C").alias("weight_kg_tier"),
        F.substring(F.trim(F.col("category")), 1, 2).alias("category_prefix"),
        F.substring(F.trim(F.col("category")), 1, 2).alias("category_prefix_2"),
    )
)

windowed = (
    base
    .select(
        "*",
        F.max("unit_cost").over(w_category).alias("category_max_unit_cost"),
        (F.col("weight_kg") - F.lag("weight_kg").over(w_all_by_unit_cost)).alias("weight_kg_change"),
        F.round(F.percent_rank().over(w_all_by_unit_cost_ties), 3).alias("unit_cost_percent_rank"),
        F.round(F.col("unit_cost") - F.avg("unit_cost").over(w_category), 2).alias("unit_cost_vs_category_avg"),
        F.round(F.col("weight_kg") / F.nullif(F.sum("weight_kg").over(w_category), F.lit(0)), 4).alias("weight_kg_share_of_category"),
        F.sum("unit_cost").over(w_all_by_unit_cost_prior_6).alias("unit_cost_sum_6_prior"),
    )
)

category_stats = (
    windowed
    .groupBy("category")
    .agg(F.max("weight_kg_share_of_category").alias("max_weight_kg_share_of_category"))
)

vs_group = (
    windowed.alias("s")
    .join(category_stats.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.weight_kg_share_of_category") == F.col("g.max_weight_kg_share_of_category"))
    .select(
        "s.*",
        "g.max_weight_kg_share_of_category",
        F.round(F.col("s.weight_kg_share_of_category") - F.col("g.max_weight_kg_share_of_category"), 2).alias("weight_kg_share_of_category_minus_max"),
    )
)

qualified = (
    vs_group
    .crossJoin(vs_group.agg(F.min("unit_cost_vs_category_avg").alias("overall_min_unit_cost_vs_category_avg")))
    .filter(F.col("unit_cost_vs_category_avg") > F.col("overall_min_unit_cost_vs_category_avg"))
    .drop("overall_min_unit_cost_vs_category_avg")
)

filtered = (
    qualified
    .crossJoin(qualified.agg(F.avg("unit_cost").alias("overall_avg_unit_cost")))
    .filter(F.col("unit_cost") > F.col("overall_avg_unit_cost"))
    .drop("overall_avg_unit_cost")
)

result = (
    filtered
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "weight_kg_tier",
        "category_prefix",
        "category_prefix_2",
        "weight_kg_change",
        "unit_cost_percent_rank",
        "unit_cost_vs_category_avg",
        "weight_kg_share_of_category",
        "unit_cost_sum_6_prior",
        "weight_kg_share_of_category_minus_max",
        F.concat(F.col("weight_kg_tier"), F.lit("#"), F.col("id").cast("string")).alias("weight_kg_tier_ref"),
        F.abs(F.col("unit_cost_percent_rank") - 0.7).alias("unit_cost_percent_rank_distance"),
    )
    .orderBy("weight_kg_change", "id")
    .limit(10)
)
