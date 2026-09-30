from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_cost_prior_3 = Window.orderBy(F.col("unit_cost").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_is_active = Window.partitionBy("is_active")
w_all_by_unit_cost_ties = Window.orderBy(F.col("unit_cost").asc_nulls_last())
w_all_by_unit_cost = Window.orderBy(F.col("unit_cost").asc_nulls_last(), "id")
w_is_active_by_unit_cost = Window.partitionBy("is_active").orderBy("unit_cost", "id")
w_is_active_by_unit_cost_full = Window.partitionBy("is_active").orderBy(F.col("unit_cost").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_unit_cost_dense_rank_unit_co = Window.partitionBy("unit_cost_dense_rank", "unit_cost_dense_rank_2").orderBy(F.col("is_active_max_unit_cost").desc())
w_unit_cost_dense_rank = Window.partitionBy("unit_cost_dense_rank")

base = (
    df_skus
    .filter((F.col("weight_kg") >= 6.4) & (F.col("is_active")))
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.substring(F.trim(F.col("category")), 1, 2).alias("category_prefix"),
    )
)

ranked = (
    base
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "is_active",
        "category_prefix",
        F.round(F.avg("unit_cost").over(w_all_by_unit_cost_prior_3), 2).alias("unit_cost_avg_3_prior"),
        F.round(F.avg("weight_kg").over(w_is_active), 2).alias("is_active_avg_weight_kg"),
        F.dense_rank().over(w_all_by_unit_cost_ties).alias("unit_cost_dense_rank"),
        F.round(F.col("weight_kg") - F.avg("weight_kg").over(w_is_active), 2).alias("weight_kg_vs_is_active_avg"),
        F.lag("unit_cost", 1, 0).over(w_all_by_unit_cost).alias("prev_unit_cost"),
        F.dense_rank().over(w_is_active_by_unit_cost).alias("unit_cost_dense_rank_2"),
        (F.col("unit_cost") - F.lag("unit_cost").over(w_all_by_unit_cost)).alias("unit_cost_change"),
        F.round(F.col("unit_cost") / F.nullif(F.sum("unit_cost").over(w_is_active), F.lit(0)), 4).alias("unit_cost_share_of_is_active"),
        F.max("unit_cost").over(w_is_active).alias("is_active_max_unit_cost"),
        F.first("weight_kg").over(w_is_active_by_unit_cost_full).alias("first_weight_kg"),
        F.lead("weight_kg", 1, 0).over(w_all_by_unit_cost).alias("next_weight_kg"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "sku_code",
        "is_active",
        "category_prefix",
        "is_active_avg_weight_kg",
        "unit_cost_dense_rank",
        "unit_cost_dense_rank_2",
        "unit_cost_change",
        "unit_cost_share_of_is_active",
        "is_active_max_unit_cost",
        "first_weight_kg",
        "next_weight_kg",
        F.round(F.percent_rank().over(w_unit_cost_dense_rank_unit_co), 3).alias("is_active_max_unit_cost_percent_rank"),
        F.round(F.col("prev_unit_cost") - F.avg("prev_unit_cost").over(w_unit_cost_dense_rank), 2).alias("prev_unit_cost_vs_unit_cost_dense_rank_avg"),
        F.round(F.col("unit_cost_dense_rank_2") - F.avg("unit_cost_dense_rank_2").over(w_unit_cost_dense_rank), 2).alias("unit_cost_dense_rank_2_vs_unit_cost_dense_rank_a"),
        F.round(F.col("is_active_max_unit_cost") / F.nullif(F.sum("is_active_max_unit_cost").over(w_unit_cost_dense_rank), F.lit(0)), 4).alias("is_active_max_unit_cost_share_of_unit_cost_dense"),
    )
)

result = (
    windowed
    .select(
        "id",
        "sku_code",
        "is_active",
        "category_prefix",
        "is_active_avg_weight_kg",
        "unit_cost_dense_rank_2",
        "unit_cost_share_of_is_active",
        "is_active_max_unit_cost",
        "first_weight_kg",
        "next_weight_kg",
        "is_active_max_unit_cost_percent_rank",
        "unit_cost_dense_rank_2_vs_unit_cost_dense_rank_a",
    )
    .orderBy("next_weight_kg")
)
