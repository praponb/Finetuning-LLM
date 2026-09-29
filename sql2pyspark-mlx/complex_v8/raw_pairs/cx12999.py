from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_is_active_label_by = Window.partitionBy("is_active", "is_active_label").orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_is_active = Window.partitionBy("category", "is_active")
w_dedup = Window.partitionBy("is_active_label").orderBy("first_unit_cost", "id")

prepared = (
    df_skus
    .filter(~F.col("is_active"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        F.abs(F.col("unit_cost") - 231.6).alias("unit_cost_distance"),
        F.array_contains(F.col("labels"), "bulky").alias("has_bulky"),
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "is_active",
        "unit_cost_distance",
        "has_bulky",
        "is_active_label",
        F.first("unit_cost").over(w_is_active_is_active_label_by).alias("first_unit_cost"),
        F.count(F.lit(1)).over(w_category_is_active).alias("rows_in_category_is_active"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

totals = (
    latest
    .groupBy("is_active_label")
    .agg(
        F.sum(F.when(F.lower(F.col("sku_code")).like("%el%"), F.col("rows_in_category_is_active"))).alias("rows_in_category_is_active_lower_sku_code_like_e"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("first_unit_cost"), 2).alias("total_first_unit_cost"),
        F.count(F.when(~F.col("is_active"), 1)).alias("n_is_active_false"),
    )
)

without_matches = (
    totals
    .crossJoin(totals.agg(F.avg("rows_in_category_is_active_lower_sku_code_like_e").alias("overall_avg_rows_in_category_is_active_lower_sku")))
    .filter(F.col("rows_in_category_is_active_lower_sku_code_like_e") > F.col("overall_avg_rows_in_category_is_active_lower_sku"))
    .drop("overall_avg_rows_in_category_is_active_lower_sku")
)

enriched = (
    without_matches
    .select(
        "is_active_label",
        "rows_in_category_is_active_lower_sku_code_like_e",
        "total_first_unit_cost",
        "n_is_active_false",
        F.length(F.trim(F.col("is_active_label"))).alias("is_active_label_len"),
        (F.col("distinct_ids") - F.col("total_first_unit_cost")).alias("distinct_ids_minus_total_first_unit_cost"),
    )
)

without_matches_2 = (
    enriched
    .crossJoin(enriched.agg(F.min("total_first_unit_cost").alias("overall_min_total_first_unit_cost")))
    .filter(F.col("total_first_unit_cost") > F.col("overall_min_total_first_unit_cost"))
    .drop("overall_min_total_first_unit_cost")
)

result = (
    without_matches_2
    .filter(F.col("is_active_label_len") >= 4)
    .select(
        "is_active_label",
        "rows_in_category_is_active_lower_sku_code_like_e",
        "total_first_unit_cost",
        "n_is_active_false",
        F.substring(F.trim(F.col("is_active_label")), 1, 3).alias("is_active_label_prefix"),
        F.abs(F.col("n_is_active_false") - 6).alias("n_is_active_false_distance"),
    )
    .orderBy(
        F.col("rows_in_category_is_active_lower_sku_code_like_e").desc(),
        F.col("n_is_active_false").desc(),
    )
)
