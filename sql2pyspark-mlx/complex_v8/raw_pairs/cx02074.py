from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_label = Window.partitionBy("is_active_label")
w_all_by_unit_cost_moving_3 = Window.orderBy(F.col("unit_cost").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_category_is_active_label_by = Window.partitionBy("category", "is_active_label").orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_is_active_label_by_2 = Window.partitionBy("category", "is_active_label").orderBy("weight_kg", "id")
w_category_is_active_label_by_3 = Window.partitionBy("category", "is_active_label").orderBy(F.col("weight_kg").desc())
w_category_is_active_label_by_4 = Window.partitionBy("category", "is_active_label").orderBy(F.col("weight_kg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_skus = (
    df_skus
    .filter(F.col("weight_kg").between(1.8, 14.6))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
    )
)

with_windows = (
    valid_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "labels",
        F.round(F.col("weight_kg") / F.nullif(F.sum("weight_kg").over(w_is_active_label), F.lit(0)), 4).alias("weight_kg_share_of_is_active_label"),
        F.max("weight_kg").over(w_all_by_unit_cost_moving_3).alias("weight_kg_max_4"),
        F.last("weight_kg", ignorenulls=True).over(w_category_is_active_label_by).alias("last_known_weight_kg"),
        F.lag("weight_kg").over(w_category_is_active_label_by_2).alias("prev_weight_kg"),
        F.rank().over(w_category_is_active_label_by_3).alias("weight_kg_rank"),
        F.last("weight_kg", ignorenulls=True).over(w_category_is_active_label_by_4).alias("last_known_weight_kg_2"),
    )
)

annotated = (
    with_windows
    .filter(F.col("last_known_weight_kg") < 14.6)
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "labels",
        "weight_kg_rank",
        "last_known_weight_kg_2",
        F.coalesce(F.col("weight_kg_rank"), F.lit(0)).alias("weight_kg_rank_filled"),
        F.round(F.col("last_known_weight_kg_2") * 0.25, 2).alias("last_known_weight_kg_2_adj"),
    )
)

annotated_2 = (
    annotated
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "labels",
        "weight_kg_rank",
        "last_known_weight_kg_2",
        "weight_kg_rank_filled",
        "last_known_weight_kg_2_adj",
        F.coalesce(F.col("last_known_weight_kg_2"), F.lit(0.0)).alias("last_known_weight_kg_2_filled"),
        F.greatest(F.col("weight_kg_rank"), F.col("weight_kg_rank_filled")).alias("greatest_weight_kg_rank_weight_kg_rank_filled"),
        F.upper(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
    )
)

result = (
    annotated_2
    .groupingSets(
        [["weight_kg_rank", "greatest_weight_kg_rank_weight_kg_rank_filled", "weight_kg_rank_filled"], ["weight_kg_rank"], []],
        "weight_kg_rank", "greatest_weight_kg_rank_weight_kg_rank_filled", "weight_kg_rank_filled",
    )
    .agg(
        F.round(F.sum("greatest_weight_kg_rank_weight_kg_rank_filled") / F.nullif(F.sum("unit_cost"), F.lit(0)), 3).alias("greatest_weight_kg_rank_weight_kg_rank_filled_to"),
        F.round(F.sum("last_known_weight_kg_2_filled"), 2).alias("total_last_known_weight_kg_2_filled"),
    )
    .orderBy(
        F.col("weight_kg_rank").asc_nulls_last(),
        F.col("greatest_weight_kg_rank_weight_kg_rank_filled").asc_nulls_last(),
        F.col("weight_kg_rank_filled").asc_nulls_last(),
    )
)
