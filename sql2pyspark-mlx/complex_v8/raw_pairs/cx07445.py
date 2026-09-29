from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active = Window.partitionBy("is_active")
w_all_by_unit_cost_ties = Window.orderBy("unit_cost")
w_is_active_by_unit_cost_ties = Window.partitionBy("is_active").orderBy("unit_cost")
w_is_active_by_weight_kg_full = Window.partitionBy("is_active").orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_cost = Window.orderBy("unit_cost", "id")

cleaned_skus = (
    df_skus
    .filter(~F.col("is_active"))
    .select(
        "id",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.concat(F.col("sku_code"), F.lit("#"), F.col("id").cast("string")).alias("sku_code_ref"),
        F.concat_ws("|", F.col("category"), F.col("sku_code")).alias("category_sku_code_label"),
    )
)

screened = (
    cleaned_skus
    .crossJoin(cleaned_skus.agg(F.min("weight_kg").alias("overall_min_weight_kg")))
    .filter(F.col("weight_kg") > F.col("overall_min_weight_kg"))
    .drop("overall_min_weight_kg")
)

windowed = (
    screened
    .select(
        "id",
        "weight_kg",
        "labels",
        F.round(F.percent_rank().over(w_all_by_unit_cost_ties), 3).alias("unit_cost_percent_rank"),
        F.dense_rank().over(w_is_active_by_unit_cost_ties).alias("unit_cost_dense_rank"),
        F.count(F.lit(1)).over(w_is_active).alias("rows_in_is_active"),
        F.min("weight_kg").over(w_is_active).alias("is_active_min_weight_kg"),
        F.first("weight_kg").over(w_is_active_by_weight_kg_full).alias("first_weight_kg"),
        F.round(F.percent_rank().over(w_all_by_unit_cost), 3).alias("unit_cost_percent_rank_2"),
    )
)

derived = (
    windowed
    .select(
        "*",
        F.abs(F.col("rows_in_is_active") - 10).alias("rows_in_is_active_distance"),
        F.size(F.col("labels")).alias("labels_count"),
        F.when(F.col("weight_kg") >= 18.5, "large").when(F.col("weight_kg") >= 6.5, "medium").otherwise("small").alias("weight_kg_tier"),
    )
)

leaders = (
    derived
    .filter(F.col("unit_cost_dense_rank") == 1)
    .drop("unit_cost_dense_rank")
)

enriched = (
    leaders
    .filter(F.col("unit_cost_percent_rank") < 0.6)
    .select(
        "id",
        "weight_kg",
        "labels",
        "unit_cost_percent_rank",
        "is_active_min_weight_kg",
        "first_weight_kg",
        "unit_cost_percent_rank_2",
        "rows_in_is_active_distance",
        "labels_count",
        F.round(F.col("is_active_min_weight_kg") * 1.1, 2).alias("is_active_min_weight_kg_adj"),
        F.round(F.col("unit_cost_percent_rank_2") * 0.25, 2).alias("unit_cost_percent_rank_2_adj"),
        F.greatest(F.col("first_weight_kg"), F.col("labels_count")).alias("greatest_first_weight_kg_labels_count"),
    )
)

result = (
    enriched
    .select(
        "id",
        "weight_kg",
        "labels",
        "unit_cost_percent_rank",
        "is_active_min_weight_kg",
        "unit_cost_percent_rank_2",
        "rows_in_is_active_distance",
        "labels_count",
        "unit_cost_percent_rank_2_adj",
        "greatest_first_weight_kg_labels_count",
    )
    .orderBy(
        F.col("labels_count").desc(),
        F.col("greatest_first_weight_kg_labels_count").asc_nulls_last(),
    )
)
