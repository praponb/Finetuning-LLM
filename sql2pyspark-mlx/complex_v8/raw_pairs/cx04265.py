from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_category = Window.partitionBy("is_active", "category")
w_category_by_weight_kg = Window.partitionBy("category").orderBy("weight_kg", "id")
w_category_is_active_by_unit_c = Window.partitionBy("category", "is_active").orderBy(F.col("unit_cost").asc_nulls_last(), "id")
w_category_by_weight_kg_ties = Window.partitionBy("category").orderBy("weight_kg")
w_is_active = Window.partitionBy("is_active")
w_category_is_active_by_unit_c_2 = Window.partitionBy("category", "is_active").orderBy(F.col("unit_cost").asc_nulls_last())
w_category_is_active_by_unit_c_3 = Window.partitionBy("category", "is_active").orderBy(F.col("unit_cost").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_weight_kg = Window.orderBy(F.col("weight_kg").desc(), "id")
w_category_is_active_by_unit_c_4 = Window.partitionBy("category", "is_active").orderBy(F.col("unit_cost").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_weight_kg_quartile = Window.partitionBy("category", "weight_kg_quartile").orderBy(F.col("is_active_category_avg_weight_kg").desc(), "id")
w_all_by_rows_in_is_active_tie = Window.orderBy("rows_in_is_active")
w_category_weight_kg_quartile_2 = Window.partitionBy("category", "weight_kg_quartile").orderBy(F.col("is_active_category_avg_weight_kg").desc(), "id").rowsBetween(-3, Window.currentRow)
w_category_weight_kg_quartile_3 = Window.partitionBy("category", "weight_kg_quartile").orderBy(F.col("is_active_category_avg_weight_kg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_rows_in_is_active = Window.orderBy("rows_in_is_active", "id")
w_weight_kg_tercile_unit_cost = Window.partitionBy("weight_kg_tercile", "unit_cost_dense_rank").orderBy("weight_kg_cume_dist", "id").rowsBetween(-3, Window.currentRow)
w_median_unit_cost_dense_rank = Window.partitionBy("median_unit_cost_dense_rank", "rn").orderBy("median_unit_cost_dense_rank", "rn")
w_median_unit_cost_dense_rank_2 = Window.partitionBy("median_unit_cost_dense_rank")

base_skus = df_skus.filter(F.col("is_active"))

windowed = (
    base_skus
    .select(
        "id",
        "category",
        "weight_kg",
        "labels",
        F.ntile(4).over(w_category_by_weight_kg).alias("weight_kg_quartile"),
        F.lag("unit_cost").over(w_category_is_active_by_unit_c).alias("prev_unit_cost"),
        F.round(F.cume_dist().over(w_category_by_weight_kg_ties), 3).alias("weight_kg_cume_dist"),
        F.count(F.lit(1)).over(w_is_active).alias("rows_in_is_active"),
        F.dense_rank().over(w_category_is_active_by_unit_c_2).alias("unit_cost_dense_rank"),
        F.round(F.avg("weight_kg").over(w_is_active_category), 2).alias("is_active_category_avg_weight_kg"),
        F.round(F.col("unit_cost") / F.nullif(F.sum("unit_cost").over(w_is_active_category), F.lit(0)), 4).alias("unit_cost_share_of_is_active_category"),
        F.first("unit_cost", ignorenulls=True).over(w_category_is_active_by_unit_c_3).alias("first_known_unit_cost"),
        F.ntile(3).over(w_all_by_weight_kg).alias("weight_kg_tercile"),
        F.nth_value("unit_cost", 2).over(w_category_is_active_by_unit_c_4).alias("second_unit_cost"),
        F.last("unit_cost", ignorenulls=True).over(w_category_is_active_by_unit_c_3).alias("last_known_unit_cost"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.row_number().over(w_category_weight_kg_quartile).alias("rn"),
        F.dense_rank().over(w_all_by_rows_in_is_active_tie).alias("rows_in_is_active_dense_rank"),
        F.round(F.avg("weight_kg_cume_dist").over(w_category_weight_kg_quartile_2), 2).alias("weight_kg_cume_dist_avg_4"),
        F.first("weight_kg_tercile").over(w_category_weight_kg_quartile_3).alias("first_weight_kg_tercile"),
        F.lead("unit_cost_dense_rank", 2).over(w_all_by_rows_in_is_active).alias("next2_unit_cost_dense_rank"),
        F.sum("weight_kg_quartile").over(w_weight_kg_tercile_unit_cost).alias("weight_kg_quartile_sum_4"),
        F.first("prev_unit_cost").over(w_category_weight_kg_quartile_3).alias("first_prev_unit_cost"),
    )
)

summary = (
    ranked
    .groupBy("rn")
    .agg(
        F.percentile_approx("unit_cost_dense_rank", 0.5).alias("median_unit_cost_dense_rank"),
        F.round(100 * F.count(F.when(~F.col("category").isin("Food", "Beauty"), 1)) / F.count("*"), 1).alias("pct_category_not_in_food_beauty"),
        F.array_sort(F.collect_set("category")).alias("category_list"),
    )
)

with_trends = (
    summary
    .select(
        "*",
        F.rank().over(w_median_unit_cost_dense_rank).alias("median_unit_cost_dense_rank_rank"),
        F.ntile(3).over(w_median_unit_cost_dense_rank).alias("median_unit_cost_dense_rank_tercile"),
        (F.col("median_unit_cost_dense_rank") - F.lag("median_unit_cost_dense_rank").over(w_median_unit_cost_dense_rank)).alias("median_unit_cost_dense_rank_change"),
        F.count(F.lit(1)).over(w_median_unit_cost_dense_rank_2).alias("rows_in_median_unit_cost_dense_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("median_unit_cost_dense_rank_tercile") < 7)
    .select(
        "rn",
        "median_unit_cost_dense_rank",
        "pct_category_not_in_food_beauty",
        "category_list",
        "median_unit_cost_dense_rank_tercile",
        "median_unit_cost_dense_rank_change",
        "rows_in_median_unit_cost_dense_rank",
        (F.floor(F.col("rn") / 2) * 2).alias("rn_bucket"),
    )
    .orderBy(
        F.col("pct_category_not_in_food_beauty").desc(),
        F.col("median_unit_cost_dense_rank_tercile").asc_nulls_last(),
    )
)
