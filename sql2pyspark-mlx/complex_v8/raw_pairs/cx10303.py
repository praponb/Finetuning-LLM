from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_n_lower_sku_code_li = Window.partitionBy("category", "n_lower_sku_code_like_el")
w_all_by_min_weight_kg = Window.orderBy("min_weight_kg", "sku_code_len", "is_active", "category")
w_n_lower_sku_code_like_el = Window.partitionBy("n_lower_sku_code_like_el")
w_n_lower_sku_code_like_el_is = Window.partitionBy("n_lower_sku_code_like_el", "is_active_label").orderBy("total_unit_cost", "sku_code_len", "is_active", "category").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_skus = (
    df_skus
    .filter((F.size(F.col("labels")) >= 1) & (F.length(F.trim(F.col("sku_code"))) >= 4))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.lower(F.regexp_replace(F.trim(F.col("category")), " ", "_")).alias("category_slug"),
        F.substring(F.trim(F.col("category")), 1, 3).alias("category_prefix"),
        F.length(F.trim(F.col("sku_code"))).alias("sku_code_len"),
    )
)

summary = (
    filtered_skus
    .groupBy("sku_code_len", "is_active", "category")
    .agg(
        F.count(F.when(F.lower(F.col("sku_code")).like("%el%"), 1)).alias("n_lower_sku_code_like_el"),
        F.round(F.sum("weight_kg") / F.nullif(F.sum("sku_code_len"), F.lit(0)), 3).alias("weight_kg_to_sku_code_len_ratio"),
        F.round(F.sum("unit_cost"), 2).alias("total_unit_cost"),
        F.min("weight_kg").alias("min_weight_kg"),
        F.sum("unit_cost").alias("total_unit_cost_2"),
    )
)

with_trends = (
    summary
    .select(
        "sku_code_len",
        "is_active",
        "category",
        "n_lower_sku_code_like_el",
        "weight_kg_to_sku_code_len_ratio",
        "total_unit_cost",
        "min_weight_kg",
        F.round(F.col("min_weight_kg") - F.avg("min_weight_kg").over(w_category_n_lower_sku_code_li), 2).alias("min_weight_kg_vs_category_n_lower_sku_code_like_"),
        F.lag("sku_code_len").over(w_all_by_min_weight_kg).alias("prev_sku_code_len"),
        F.round(F.avg("total_unit_cost_2").over(w_category_n_lower_sku_code_li), 2).alias("category_n_lower_sku_code_like_el_avg_total_unit"),
        F.ntile(5).over(w_all_by_min_weight_kg).alias("min_weight_kg_quintile"),
    )
)

scored = (
    with_trends
    .select(
        "*",
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
        F.round(F.col("total_unit_cost") / F.nullif(F.col("n_lower_sku_code_like_el"), F.lit(0)), 3).alias("total_unit_cost_per_n_lower_sku_code_like_el"),
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.round(F.avg("is_active_flag").over(w_n_lower_sku_code_like_el), 2).alias("n_lower_sku_code_like_el_avg_is_active_flag"),
        F.last("is_active_flag", ignorenulls=True).over(w_n_lower_sku_code_like_el_is).alias("last_known_is_active_flag"),
        F.min("min_weight_kg_quintile").over(w_n_lower_sku_code_like_el).alias("n_lower_sku_code_like_el_min_min_weight_kg_quint"),
    )
)

scored_2 = (
    metrics
    .filter(F.col("is_active_label").isNotNull())
    .select(
        "*",
        F.when(F.col("n_lower_sku_code_like_el") >= 6, "A").when(F.col("n_lower_sku_code_like_el") >= 2, "B").otherwise("C").alias("n_lower_sku_code_like_el_tier"),
        F.abs(F.col("n_lower_sku_code_like_el") - 6).alias("n_lower_sku_code_like_el_distance"),
        F.coalesce(F.col("total_unit_cost"), F.lit(0.0)).alias("total_unit_cost_filled"),
    )
)

result = (
    scored_2
    .select(
        "sku_code_len",
        "category",
        "weight_kg_to_sku_code_len_ratio",
        "total_unit_cost",
        "prev_sku_code_len",
        "category_n_lower_sku_code_like_el_avg_total_unit",
        "min_weight_kg_quintile",
        "is_active_label",
        "total_unit_cost_per_n_lower_sku_code_like_el",
        "n_lower_sku_code_like_el_min_min_weight_kg_quint",
        "n_lower_sku_code_like_el_distance",
        "total_unit_cost_filled",
    )
    .orderBy("is_active_label", F.col("min_weight_kg_quintile").asc())
)
