from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_by_unit_cost_runni = Window.partitionBy("is_active").orderBy(F.col("unit_cost").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_cost = Window.orderBy(F.col("unit_cost").desc(), "id")
w_is_active_by_unit_cost = Window.partitionBy("is_active").orderBy(F.col("unit_cost").desc_nulls_first(), "id")
w_all_by_unit_cost_ties = Window.orderBy(F.col("unit_cost").desc())
w_all_by_unit_cost_running = Window.orderBy(F.col("unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_is_active = Window.partitionBy("category", "is_active")
w_unit_cost_seq_by_category_is = Window.partitionBy("unit_cost_seq").orderBy(F.col("category_is_active_min_weight_kg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_unit_cost_seq = Window.partitionBy("unit_cost_seq")
w_unit_cost_seq_by_category_is_2 = Window.partitionBy("unit_cost_seq").orderBy(F.col("category_is_active_min_weight_kg").desc(), "id")

base = (
    df_skus
    .filter((F.col("category") == "Beauty") & (F.col("unit_cost") >= 68.0))
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

metrics = (
    base
    .select(
        "id",
        "unit_cost",
        "is_active",
        F.max("weight_kg").over(w_is_active_by_unit_cost_runni).alias("max_weight_kg_so_far"),
        F.lead("weight_kg").over(w_all_by_unit_cost).alias("next_weight_kg"),
        (F.col("unit_cost") - F.lag("unit_cost").over(w_all_by_unit_cost)).alias("unit_cost_change"),
        F.row_number().over(w_is_active_by_unit_cost).alias("unit_cost_seq"),
        F.round(F.percent_rank().over(w_all_by_unit_cost_ties), 3).alias("unit_cost_percent_rank"),
        F.max("unit_cost").over(w_all_by_unit_cost_running).alias("max_unit_cost_so_far"),
        F.lead("weight_kg", 2).over(w_is_active_by_unit_cost).alias("next2_weight_kg"),
        F.dense_rank().over(w_all_by_unit_cost).alias("unit_cost_dense_rank"),
        F.min("weight_kg").over(w_category_is_active).alias("category_is_active_min_weight_kg"),
    )
)

derived = (
    metrics
    .select(
        "id",
        "unit_cost_change",
        "unit_cost_seq",
        "unit_cost_percent_rank",
        "max_unit_cost_so_far",
        "next2_weight_kg",
        "category_is_active_min_weight_kg",
        F.round(F.col("unit_cost_seq") / F.nullif(F.col("max_weight_kg_so_far"), F.lit(0)), 3).alias("unit_cost_seq_per_max_weight_kg_so_far"),
        F.greatest(F.col("unit_cost_dense_rank"), F.col("next2_weight_kg")).alias("greatest_unit_cost_dense_rank_next2_weight_kg"),
    )
)

sequenced = (
    derived
    .select(
        "*",
        F.nth_value("max_unit_cost_so_far", 2).over(w_unit_cost_seq_by_category_is).alias("second_max_unit_cost_so_far"),
        F.round(F.col("next2_weight_kg") / F.nullif(F.sum("next2_weight_kg").over(w_unit_cost_seq), F.lit(0)), 4).alias("next2_weight_kg_share_of_unit_cost_seq"),
        F.round(F.cume_dist().over(w_unit_cost_seq_by_category_is_2), 3).alias("category_is_active_min_weight_kg_cume_dist"),
        F.round(F.col("max_unit_cost_so_far") / F.nullif(F.sum("max_unit_cost_so_far").over(w_unit_cost_seq), F.lit(0)), 4).alias("max_unit_cost_so_far_share_of_unit_cost_seq"),
        F.round(F.avg("unit_cost_seq").over(w_unit_cost_seq), 2).alias("unit_cost_seq_avg_unit_cost_seq"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "unit_cost_change",
        "unit_cost_seq",
        "unit_cost_percent_rank",
        "max_unit_cost_so_far",
        "next2_weight_kg",
        "category_is_active_min_weight_kg",
        "greatest_unit_cost_dense_rank_next2_weight_kg",
        "second_max_unit_cost_so_far",
        "next2_weight_kg_share_of_unit_cost_seq",
        "category_is_active_min_weight_kg_cume_dist",
        "max_unit_cost_so_far_share_of_unit_cost_seq",
        F.greatest(F.col("unit_cost_seq"), F.col("unit_cost_percent_rank")).alias("greatest_unit_cost_seq_unit_cost_percent_rank"),
    )
    .orderBy("unit_cost_seq")
)
