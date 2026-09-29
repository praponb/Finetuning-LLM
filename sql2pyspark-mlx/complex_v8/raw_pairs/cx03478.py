from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_by_unit_cost = Window.partitionBy("is_active").orderBy("unit_cost", "id")
w_category_by_weight_kg_adj_fu = Window.partitionBy("category").orderBy("weight_kg_adj", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_weight_kg_adj = Window.orderBy(F.col("weight_kg_adj").desc_nulls_first(), "id")
w_category = Window.partitionBy("category")
w_is_active_by_unit_cost_ties = Window.partitionBy("is_active").orderBy("unit_cost")

filtered = (
    df_skus
    .filter(F.col("unit_cost") > 38.3)
    .select(
        "id",
        "category",
        "unit_cost",
        "labels",
        "is_active",
        F.round(F.col("weight_kg") * 0.25, 2).alias("weight_kg_adj"),
    )
)

ranked = (
    filtered
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg_adj",
        F.ntile(3).over(w_is_active_by_unit_cost).alias("unit_cost_tercile"),
        F.round(F.percent_rank().over(w_is_active_by_unit_cost), 3).alias("unit_cost_percent_rank"),
        F.nth_value("weight_kg_adj", 2).over(w_category_by_weight_kg_adj_fu).alias("second_weight_kg_adj"),
        (F.col("unit_cost") - F.lag("unit_cost").over(w_all_by_weight_kg_adj)).alias("unit_cost_change"),
        F.count(F.lit(1)).over(w_category).alias("rows_in_category"),
        F.max("weight_kg_adj").over(w_category).alias("category_max_weight_kg_adj"),
        F.round(F.col("unit_cost") - F.avg("unit_cost").over(w_category), 2).alias("unit_cost_vs_category_avg"),
        F.round(F.percent_rank().over(w_is_active_by_unit_cost_ties), 3).alias("unit_cost_percent_rank_2"),
    )
)

result = (
    ranked
    .filter(F.col("unit_cost_percent_rank") > 0.2)
    .select(
        "id",
        "category",
        "weight_kg_adj",
        "unit_cost_tercile",
        "unit_cost_percent_rank",
        "second_weight_kg_adj",
        "unit_cost_change",
        "rows_in_category",
        "category_max_weight_kg_adj",
        "unit_cost_vs_category_avg",
        "unit_cost_percent_rank_2",
        F.round(F.col("rows_in_category") / F.nullif(F.col("unit_cost_percent_rank_2"), F.lit(0)), 3).alias("rows_in_category_per_unit_cost_percent_rank_2"),
        F.substring(F.trim(F.col("category")), 1, 4).alias("category_prefix"),
    )
    .orderBy(
        F.col("unit_cost_percent_rank_2").desc_nulls_first(),
        F.col("category").asc_nulls_last(),
    )
)
