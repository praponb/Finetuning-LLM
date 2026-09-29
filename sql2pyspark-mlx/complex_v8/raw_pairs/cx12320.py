from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category = Window.partitionBy("category")
w_all_by_unit_cost_full = Window.orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_cost_running = Window.orderBy(F.col("unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_cost_moving_2 = Window.orderBy("unit_cost", "id").rowsBetween(-2, Window.currentRow)
w_all_by_unit_cost = Window.orderBy(F.col("unit_cost").desc(), "id")
w_all_by_unit_cost_2 = Window.orderBy("unit_cost", "id")
w_all_by_unit_cost_ties = Window.orderBy("unit_cost")
w_all_by_unit_cost_running_2 = Window.orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_by_unit_cost_change = Window.partitionBy("category").orderBy(F.col("unit_cost_change_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_by_category_max_uni = Window.partitionBy("category").orderBy(F.col("category_max_unit_cost").desc(), "id").rowsBetween(-2, -1)
w_category_by_first_known_unit = Window.partitionBy("category").orderBy("first_known_unit_cost", "id")
w_category_by_category_max_uni_2 = Window.partitionBy("category").orderBy(F.col("category_max_unit_cost").desc(), "id")
w_category_by_first_known_unit_2 = Window.partitionBy("category").orderBy("first_known_unit_cost")

scoped = (
    df_skus
    .filter((~F.col("is_active")) & (F.col("category") != "Beauty"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "labels",
        F.substring(F.trim(F.col("sku_code")), 1, 3).alias("sku_code_prefix"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.count(F.lit(1)).over(w_category).alias("rows_in_category"),
        F.last("unit_cost").over(w_all_by_unit_cost_full).alias("last_unit_cost"),
        F.last("unit_cost", ignorenulls=True).over(w_all_by_unit_cost_running).alias("last_known_unit_cost"),
        F.round(F.col("unit_cost") - F.avg("unit_cost").over(w_category), 2).alias("unit_cost_vs_category_avg"),
        F.sum("unit_cost").over(w_all_by_unit_cost_moving_2).alias("unit_cost_sum_3"),
        (F.col("unit_cost") - F.lag("unit_cost").over(w_all_by_unit_cost)).alias("unit_cost_change"),
        (F.col("unit_cost") - F.lag("unit_cost").over(w_all_by_unit_cost_2)).alias("unit_cost_change_2"),
        F.min("unit_cost").over(w_all_by_unit_cost_running).alias("min_unit_cost_so_far"),
        F.nth_value("unit_cost", 2).over(w_all_by_unit_cost_full).alias("second_unit_cost"),
        F.max("unit_cost").over(w_category).alias("category_max_unit_cost"),
        F.round(F.percent_rank().over(w_all_by_unit_cost_ties), 3).alias("unit_cost_percent_rank"),
        F.first("unit_cost", ignorenulls=True).over(w_all_by_unit_cost_running_2).alias("first_known_unit_cost"),
        F.round(F.cume_dist().over(w_all_by_unit_cost), 3).alias("unit_cost_cume_dist"),
        F.round(F.avg("unit_cost").over(w_category), 2).alias("category_avg_unit_cost"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "labels",
        "sku_code_prefix",
        "rows_in_category",
        "last_unit_cost",
        "last_known_unit_cost",
        "unit_cost_change",
        "unit_cost_change_2",
        "min_unit_cost_so_far",
        "second_unit_cost",
        "category_max_unit_cost",
        "first_known_unit_cost",
        "category_avg_unit_cost",
        F.count(F.lit(1)).over(w_category).alias("rows_in_category_2"),
        F.last("category_max_unit_cost").over(w_category_by_unit_cost_change).alias("last_category_max_unit_cost"),
        F.sum("unit_cost_change_2").over(w_category_by_category_max_uni).alias("unit_cost_change_2_sum_2_prior"),
        F.ntile(10).over(w_category_by_first_known_unit).alias("first_known_unit_cost_decile"),
        F.round(F.percent_rank().over(w_category_by_category_max_uni_2), 3).alias("category_max_unit_cost_percent_rank"),
        F.rank().over(w_category_by_first_known_unit_2).alias("first_known_unit_cost_rank"),
        F.round(F.cume_dist().over(w_category_by_first_known_unit), 3).alias("first_known_unit_cost_cume_dist"),
        F.lead("unit_cost_change_2").over(w_category_by_category_max_uni_2).alias("next_unit_cost_change_2"),
    )
)

summary = (
    with_trends
    .filter(F.col("last_unit_cost") <= 306.4)
    .groupBy(
        F.when(F.col("last_unit_cost") >= 277.0, "large").when(F.col("last_unit_cost") >= 108.5, "medium").otherwise("small").alias("last_unit_cost_tier"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("sku_code").isNotNull(), 1).otherwise(0)).alias("n_sku_code_is_not_null"),
        F.array_sort(F.collect_set("category")).alias("category_list"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    summary
    .filter(F.col("n_rows") < 8)
    .select(
        "last_unit_cost_tier",
        "n_rows",
        (F.col("n_rows") - F.col("n_sku_code_is_not_null")).alias("n_rows_minus_n_sku_code_is_not_null"),
        (F.floor(F.col("n_sku_code_is_not_null") / 2) * 2).alias("n_sku_code_is_not_null_bucket"),
    )
    .orderBy(F.col("n_sku_code_is_not_null_bucket").asc())
)
