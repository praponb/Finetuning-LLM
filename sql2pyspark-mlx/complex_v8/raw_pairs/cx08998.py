from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_by_weight_kg_per_u = Window.partitionBy("is_active").orderBy("weight_kg_per_unit_cost", "id")
w_is_active = Window.partitionBy("is_active")

recent_skus = (
    df_skus
    .filter(F.col("category") == "Electronics")
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "labels",
        "is_active",
        F.upper(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
        F.round(F.col("weight_kg") / F.nullif(F.col("unit_cost"), F.lit(0)), 3).alias("weight_kg_per_unit_cost"),
    )
)

windowed = (
    recent_skus
    .select(
        "*",
        F.lead("unit_cost", 2, 0).over(w_is_active_by_weight_kg_per_u).alias("next2_unit_cost"),
        F.row_number().over(w_is_active_by_weight_kg_per_u).alias("rn"),
        F.lag("unit_cost", 1, 0).over(w_is_active_by_weight_kg_per_u).alias("prev_unit_cost"),
        F.round(F.col("weight_kg_per_unit_cost") / F.nullif(F.sum("weight_kg_per_unit_cost").over(w_is_active), F.lit(0)), 4).alias("weight_kg_per_unit_cost_share_of_is_active"),
    )
)

annotated = (
    windowed
    .filter(F.array_contains(F.col("labels"), "cold"))
    .select(
        "*",
        F.substring(F.trim(F.col("sku_code")), 1, 2).alias("sku_code_prefix"),
    )
)

stats = (
    annotated
    .groupBy("is_active", "rn")
    .agg(
        F.sum(F.when(F.col("is_active"), F.col("next2_unit_cost"))).alias("next2_unit_cost_is_active_true"),
        F.min("weight_kg_per_unit_cost").alias("min_weight_kg_per_unit_cost"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("unit_cost").alias("max_unit_cost_check"),
    )
    .filter(F.col("max_unit_cost_check") > 38.3)
    .drop("max_unit_cost_check")
)

screened = (
    stats
    .crossJoin(stats.agg(F.avg("min_weight_kg_per_unit_cost").alias("overall_avg_min_weight_kg_per_unit_cost")))
    .filter(F.col("min_weight_kg_per_unit_cost") > F.col("overall_avg_min_weight_kg_per_unit_cost"))
    .drop("overall_avg_min_weight_kg_per_unit_cost")
)

tagged = (
    screened
    .select(
        "is_active",
        "rn",
        "next2_unit_cost_is_active_true",
        "min_weight_kg_per_unit_cost",
        "sku_code_list",
        "cnt",
        "distinct_ids",
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
        F.round(F.col("rn") / F.nullif(F.col("cnt"), F.lit(0)), 3).alias("rn_per_cnt"),
    )
)

flattened = (
    tagged
    .select(
        "is_active",
        "rn",
        "next2_unit_cost_is_active_true",
        "min_weight_kg_per_unit_cost",
        "cnt",
        "distinct_ids",
        "is_active_label",
        "rn_per_cnt",
        F.explode_outer("sku_code_list").alias("sku_code_list_item"),
    )
)

result = (
    flattened
    .select(
        "is_active",
        "next2_unit_cost_is_active_true",
        "min_weight_kg_per_unit_cost",
        "distinct_ids",
        "is_active_label",
        "rn_per_cnt",
        F.least(F.col("cnt"), F.col("rn_per_cnt")).alias("least_cnt_rn_per_cnt"),
    )
    .orderBy(
        F.col("least_cnt_rn_per_cnt").desc(),
        F.col("is_active_label").asc_nulls_last(),
    )
)
