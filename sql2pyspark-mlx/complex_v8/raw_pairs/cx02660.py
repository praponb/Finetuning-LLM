from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_by_median_unit_cost = Window.partitionBy("category").orderBy("median_unit_cost")
w_category_by_median_unit_cost_2 = Window.partitionBy("category").orderBy("median_unit_cost", "category")
w_all_by_median_unit_cost = Window.orderBy(F.col("median_unit_cost").desc_nulls_first(), "category")
w_category = Window.partitionBy("category")
w_all_by_median_unit_cost_movi = Window.orderBy(F.col("median_unit_cost").desc_nulls_first(), "category").rowsBetween(-1, Window.currentRow)

prepared = (
    df_skus
    .filter(F.size(F.col("labels")) >= 1)
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "is_active",
    )
)

category_stats = (
    prepared
    .groupBy("category")
    .agg(F.max("unit_cost").alias("max_unit_cost"))
)

vs_group = (
    prepared.alias("s")
    .join(category_stats.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.unit_cost") == F.col("g.max_unit_cost"))
    .select(
        "s.*",
        "g.max_unit_cost",
        F.round(F.col("s.unit_cost") - F.col("g.max_unit_cost"), 2).alias("unit_cost_minus_max"),
    )
)

summary = (
    vs_group
    .filter(F.col("unit_cost") > 38.3)
    .groupBy("category")
    .agg(
        F.array_sort(F.collect_set("category")).alias("category_list"),
        F.sum(F.when(F.col("is_active"), F.col("unit_cost")).otherwise(0)).alias("unit_cost_is_active_true"),
        F.percentile_approx("unit_cost", 0.5).alias("median_unit_cost"),
    )
)

with_windows = (
    summary
    .select(
        "category",
        "category_list",
        "median_unit_cost",
        F.dense_rank().over(w_category_by_median_unit_cost).alias("median_unit_cost_dense_rank"),
        F.row_number().over(w_category_by_median_unit_cost_2).alias("median_unit_cost_seq"),
        F.rank().over(w_all_by_median_unit_cost).alias("median_unit_cost_rank"),
        F.sum("median_unit_cost").over(w_category).alias("category_sum_median_unit_cost"),
        F.count(F.lit(1)).over(w_category).alias("rows_in_category"),
        F.max("median_unit_cost").over(w_all_by_median_unit_cost_movi).alias("median_unit_cost_max_2"),
    )
)

enriched = (
    with_windows
    .filter(F.col("category") == "Apparel")
    .select(
        "*",
        F.when(F.col("median_unit_cost_max_2") >= 720.4, "A").when(F.col("median_unit_cost_max_2") >= 360.7, "B").otherwise("C").alias("median_unit_cost_max_2_tier"),
        F.abs(F.col("median_unit_cost_rank") - 6).alias("median_unit_cost_rank_distance"),
        F.least(F.col("rows_in_category"), F.col("median_unit_cost_seq")).alias("least_rows_in_category_median_unit_cost_seq"),
    )
)

result = (
    enriched
    .filter(~F.col("category").isin("Apparel", "Electronics"))
    .select(
        "category",
        "category_list",
        "median_unit_cost",
        "median_unit_cost_seq",
        "median_unit_cost_rank",
        "rows_in_category",
        "median_unit_cost_max_2",
        "median_unit_cost_max_2_tier",
        "median_unit_cost_rank_distance",
        "least_rows_in_category_median_unit_cost_seq",
        F.coalesce(F.col("median_unit_cost_max_2"), F.lit(0.0)).alias("median_unit_cost_max_2_filled"),
    )
    .orderBy(
        F.col("median_unit_cost_max_2_tier").desc(),
        F.col("rows_in_category").asc(),
    )
)
