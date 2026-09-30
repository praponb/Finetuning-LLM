from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_by_unit_cost_runnin = Window.partitionBy("category").orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_by_unit_cost = Window.partitionBy("category").orderBy(F.col("unit_cost").desc(), "id")
w_category_by_unit_cost_full = Window.partitionBy("category").orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_by_unit_cost_ties = Window.partitionBy("category").orderBy(F.col("unit_cost").desc())

prepared = (
    df_skus
    .filter((F.col("unit_cost") >= 134.3) & (F.col("sku_code").isNotNull()))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "labels",
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.last("unit_cost", ignorenulls=True).over(w_category_by_unit_cost_runnin).alias("last_known_unit_cost"),
        (F.col("unit_cost") - F.lag("unit_cost").over(w_category_by_unit_cost)).alias("unit_cost_change"),
        F.nth_value("unit_cost", 2).over(w_category_by_unit_cost_full).alias("second_unit_cost"),
        F.dense_rank().over(w_category_by_unit_cost_ties).alias("unit_cost_dense_rank"),
    )
)

category_stats = (
    with_windows
    .groupBy("category")
    .agg(F.avg("last_known_unit_cost").alias("avg_last_known_unit_cost"))
)

outliers = (
    with_windows.alias("s")
    .join(category_stats.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.last_known_unit_cost") > F.col("g.avg_last_known_unit_cost"))
    .select(
        "s.*",
        "g.avg_last_known_unit_cost",
        F.round(F.col("s.last_known_unit_cost") - F.col("g.avg_last_known_unit_cost"), 2).alias("last_known_unit_cost_minus_avg"),
    )
)

totals = (
    outliers
    .groupBy("unit_cost_dense_rank", "category")
    .agg(
        F.round(F.avg("unit_cost_dense_rank"), 2).alias("avg_unit_cost_dense_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("unit_cost_change")).otherwise(0)).alias("unit_cost_change_category_electronics"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
    )
    .filter(F.col("cnt") >= 2)
)

filtered = (
    totals
    .crossJoin(totals.agg(F.max("unit_cost_dense_rank").alias("overall_max_unit_cost_dense_rank")))
    .filter(F.col("unit_cost_dense_rank") < F.col("overall_max_unit_cost_dense_rank"))
    .drop("overall_max_unit_cost_dense_rank")
)

tagged = (
    filtered
    .select(
        "unit_cost_dense_rank",
        "category",
        "avg_unit_cost_dense_rank",
        "distinct_ids",
        "unit_cost_change_category_electronics",
        F.length(F.trim(F.col("category"))).alias("category_len"),
        F.array_join(F.array_sort(F.col("sku_code_list")), ",").alias("sku_code_list_text"),
    )
)

scored = (
    tagged
    .select(
        "unit_cost_dense_rank",
        "category",
        "avg_unit_cost_dense_rank",
        "distinct_ids",
        "unit_cost_change_category_electronics",
        "sku_code_list_text",
        F.round(F.col("unit_cost_dense_rank") / F.nullif(F.col("category_len"), F.lit(0)), 3).alias("unit_cost_dense_rank_per_category_len"),
    )
)

result = (
    scored
    .select(
        "unit_cost_dense_rank",
        "category",
        "avg_unit_cost_dense_rank",
        "distinct_ids",
        "unit_cost_change_category_electronics",
        "unit_cost_dense_rank_per_category_len",
        F.greatest(F.col("distinct_ids"), F.col("unit_cost_dense_rank")).alias("greatest_distinct_ids_unit_cost_dense_rank"),
        F.substring(F.trim(F.col("sku_code_list_text")), 1, 3).alias("sku_code_list_text_prefix"),
    )
    .orderBy(F.col("unit_cost_change_category_electronics").desc())
)
