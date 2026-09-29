from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_n_sku_code_is_not_n = Window.partitionBy("category", "n_sku_code_is_not_null_distance").orderBy("n_sku_code_is_not_null_distance", "category")
w_category = Window.partitionBy("category")
w_next_n_sku_code_is_not_null = Window.partitionBy("next_n_sku_code_is_not_null", "category")
w_next_n_sku_code_is_not_null_2 = Window.partitionBy("next_n_sku_code_is_not_null", "category").orderBy("n_sku_code_is_not_null_distance_percent_rank", "category")
w_next_n_sku_code_is_not_null_3 = Window.partitionBy("next_n_sku_code_is_not_null", "category").orderBy("n_sku_code_is_not_null_distance_percent_rank", "category").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_skus
    .filter((F.lower(F.col("sku_code")).like("%p-%")) & (~F.col("category").isin("Beauty", "Toys")))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        F.concat(F.col("sku_code"), F.lit("#"), F.col("id").cast("string")).alias("sku_code_ref"),
        F.round(F.col("weight_kg") / F.nullif(F.col("unit_cost"), F.lit(0)), 3).alias("weight_kg_per_unit_cost"),
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
    )
)

totals = (
    scoped
    .filter(F.col("category") == "Food")
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("sku_code").isNotNull(), 1).otherwise(0)).alias("n_sku_code_is_not_null"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
    )
)

scored = (
    totals
    .select(
        "*",
        F.abs(F.col("n_sku_code_is_not_null") - 4).alias("n_sku_code_is_not_null_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("category")), " ", "_")).alias("category_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("category")), " ", "_")).alias("category_slug_2"),
    )
)

sequenced = (
    scored
    .select(
        "category",
        "n_sku_code_is_not_null",
        "sku_code_list",
        "category_slug",
        "category_slug_2",
        F.lead("n_sku_code_is_not_null", 1, 0).over(w_category_n_sku_code_is_not_n).alias("next_n_sku_code_is_not_null"),
        F.round(F.percent_rank().over(w_category_n_sku_code_is_not_n), 3).alias("n_sku_code_is_not_null_distance_percent_rank"),
        F.round(F.col("n_sku_code_is_not_null") / F.nullif(F.sum("n_sku_code_is_not_null").over(w_category), F.lit(0)), 4).alias("n_sku_code_is_not_null_share_of_category"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.count(F.lit(1)).over(w_next_n_sku_code_is_not_null).alias("rows_in_next_n_sku_code_is_not_null_category"),
        F.lead("next_n_sku_code_is_not_null").over(w_next_n_sku_code_is_not_null_2).alias("next_next_n_sku_code_is_not_null"),
        F.round(F.avg("next_n_sku_code_is_not_null").over(w_next_n_sku_code_is_not_null), 2).alias("next_n_sku_code_is_not_null_category_avg_next_n_"),
        F.round(F.col("n_sku_code_is_not_null") / F.nullif(F.sum("n_sku_code_is_not_null").over(w_next_n_sku_code_is_not_null), F.lit(0)), 4).alias("n_sku_code_is_not_null_share_of_next_n_sku_code_"),
        F.count(F.lit(1)).over(w_next_n_sku_code_is_not_null_3).alias("running_count"),
    )
)

with_activity = (
    metrics
    .crossJoin(metrics.agg(F.max("next_next_n_sku_code_is_not_null").alias("overall_max_next_next_n_sku_code_is_not_null")))
    .filter(F.col("next_next_n_sku_code_is_not_null") < F.col("overall_max_next_next_n_sku_code_is_not_null"))
    .drop("overall_max_next_next_n_sku_code_is_not_null")
)

result = (
    with_activity
    .filter(F.col("next_n_sku_code_is_not_null_category_avg_next_n_") < 6.5)
    .select(
        "category",
        "n_sku_code_is_not_null",
        "sku_code_list",
        "category_slug",
        "category_slug_2",
        "next_n_sku_code_is_not_null",
        "n_sku_code_is_not_null_distance_percent_rank",
        "n_sku_code_is_not_null_share_of_next_n_sku_code_",
        F.least(F.col("running_count"), F.col("n_sku_code_is_not_null")).alias("least_running_count_n_sku_code_is_not_null"),
        (F.floor(F.col("n_sku_code_is_not_null") / 2) * 2).alias("n_sku_code_is_not_null_bucket"),
    )
)
