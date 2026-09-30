from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category = Window.partitionBy("category")
w_is_active_category_by_unit_c = Window.partitionBy("is_active", "category").orderBy(F.col("unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_active_weight_kg_tier = Window.partitionBy("is_active", "weight_kg_tier")
w_weight_kg_tier_category_by_u = Window.partitionBy("weight_kg_tier", "category").orderBy(F.col("unit_cost").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_skus
    .filter(F.lower(F.col("sku_code")).like("%ty%"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.array_contains(F.col("labels"), "bulky").alias("has_bulky"),
        (F.col("weight_kg") + F.col("unit_cost")).alias("weight_kg_plus_unit_cost"),
        F.when(F.col("weight_kg") >= 15.2, "A").when(F.col("weight_kg") >= 12.4, "B").otherwise("C").alias("weight_kg_tier"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.sum("weight_kg_plus_unit_cost").over(w_category).alias("category_sum_weight_kg_plus_unit_cost"),
        F.first("unit_cost").over(w_is_active_category_by_unit_c).alias("first_unit_cost"),
        F.round(F.avg("weight_kg").over(w_category), 2).alias("category_avg_weight_kg"),
        F.count(F.lit(1)).over(w_category).alias("rows_in_category"),
    )
)

with_windows = (
    sequenced
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "labels",
        "is_active",
        "has_bulky",
        "weight_kg_plus_unit_cost",
        "weight_kg_tier",
        "category_sum_weight_kg_plus_unit_cost",
        "first_unit_cost",
        "rows_in_category",
        F.count(F.lit(1)).over(w_is_active_weight_kg_tier).alias("rows_in_is_active_weight_kg_tier"),
        F.size(F.collect_set("category").over(w_is_active_weight_kg_tier)).alias("distinct_category_in_is_active_weight_kg_tier"),
        F.last("category_avg_weight_kg", ignorenulls=True).over(w_weight_kg_tier_category_by_u).alias("last_known_category_avg_weight_kg"),
    )
)

summary = (
    with_windows
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("category").isin("Beauty", "Apparel"), 1).otherwise(0)).alias("n_category_in_beauty_apparel"),
        F.count("*").alias("row_count"),
        F.round(F.sum("first_unit_cost") / F.nullif(F.sum("category_sum_weight_kg_plus_unit_cost"), F.lit(0)), 3).alias("first_unit_cost_to_category_sum_weight_kg_plus_u"),
    )
)

filtered = (
    summary
    .crossJoin(summary.agg(F.avg("n_category_in_beauty_apparel").alias("overall_avg_n_category_in_beauty_apparel")))
    .filter(F.col("n_category_in_beauty_apparel") > F.col("overall_avg_n_category_in_beauty_apparel"))
    .drop("overall_avg_n_category_in_beauty_apparel")
)

scored = (
    filtered
    .filter(F.col("n_category_in_beauty_apparel") >= 4)
    .select(
        "is_active",
        "row_count",
        "first_unit_cost_to_category_sum_weight_kg_plus_u",
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
        F.abs(F.col("row_count") - 8).alias("row_count_distance"),
        (F.floor(F.col("row_count") / 2) * 2).alias("row_count_bucket"),
    )
)

result = (
    scored
    .groupBy("row_count_distance", "row_count")
    .pivot("is_active_label", ["yes", "no"])
    .agg(F.max("row_count_bucket"))
    .orderBy("row_count_distance", "row_count")
)
