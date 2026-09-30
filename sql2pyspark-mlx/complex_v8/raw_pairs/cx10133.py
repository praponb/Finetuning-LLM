from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_label_category_by = Window.partitionBy("is_active_label", "category").orderBy(F.col("total_unit_cost").asc_nulls_last(), "is_active", "is_active_label", "category")
w_is_active_label_category_by_2 = Window.partitionBy("is_active_label", "category").orderBy(F.col("total_unit_cost").asc_nulls_last(), "is_active", "is_active_label", "category").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_skus = (
    df_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
        F.round(F.col("weight_kg") * 1.07, 2).alias("weight_kg_adj"),
    )
)

grouped = (
    recent_skus
    .groupBy("is_active", "is_active_label", "category")
    .agg(
        F.sum("unit_cost").alias("total_unit_cost"),
        F.array_sort(F.collect_set("category")).alias("category_list"),
        F.countDistinct("is_active_label").alias("distinct_is_active_labels"),
        F.count(F.when(F.col("category") == "Apparel", 1)).alias("n_category_apparel"),
        F.round(100 * F.count(F.when(F.lower(F.col("sku_code")).like("%ap%"), 1)) / F.count("*"), 1).alias("pct_lower_sku_code_like_ap"),
    )
)

metrics = (
    grouped
    .select(
        "*",
        F.lag("total_unit_cost").over(w_is_active_label_category_by).alias("prev_total_unit_cost"),
        F.lag("distinct_is_active_labels").over(w_is_active_label_category_by).alias("prev_distinct_is_active_labels"),
        F.first("distinct_is_active_labels", ignorenulls=True).over(w_is_active_label_category_by_2).alias("first_known_distinct_is_active_labels"),
    )
)

scored = (
    metrics
    .filter(F.size(F.col("category_list")) >= 2)
    .select(
        "*",
        F.greatest(F.col("n_category_apparel"), F.col("prev_total_unit_cost")).alias("greatest_n_category_apparel_prev_total_unit_cost"),
        F.lower(F.regexp_replace(F.trim(F.col("category")), " ", "_")).alias("category_slug"),
        F.greatest(F.col("total_unit_cost"), F.col("n_category_apparel")).alias("greatest_total_unit_cost_n_category_apparel"),
    )
)

result = (
    scored
    .select(
        "is_active",
        "is_active_label",
        "category",
        "total_unit_cost",
        "category_list",
        "n_category_apparel",
        "pct_lower_sku_code_like_ap",
        "prev_total_unit_cost",
        "prev_distinct_is_active_labels",
        "greatest_n_category_apparel_prev_total_unit_cost",
        "greatest_total_unit_cost_n_category_apparel",
        F.greatest(F.col("prev_total_unit_cost"), F.col("n_category_apparel")).alias("greatest_prev_total_unit_cost_n_category_apparel"),
    )
    .orderBy(
        F.col("greatest_total_unit_cost_n_category_apparel").desc(),
        F.col("greatest_n_category_apparel_prev_total_unit_cost").desc(),
    )
)
