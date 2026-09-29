from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_unit_cost_tier_2_category_by = Window.partitionBy("unit_cost_tier_2", "category").orderBy(F.col("move_operator_count").desc())
w_category_unit_cost_tier_by_w = Window.partitionBy("category", "unit_cost_tier").orderBy("weight_kg", "id")
w_move_count_move_operator_cou = Window.partitionBy("move_count", "move_operator_count").orderBy("move_operator_count", "id")
w_unit_cost_tier_2 = Window.partitionBy("unit_cost_tier_2")
w_unit_cost_tier_2_category_by_2 = Window.partitionBy("unit_cost_tier_2", "category").orderBy(F.col("move_operator_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_unit_cost_tier_by_w_2 = Window.partitionBy("category", "unit_cost_tier").orderBy("weight_kg")
w_is_active_unit_cost_tier_by = Window.partitionBy("is_active", "unit_cost_tier").orderBy(F.col("max_move_count_so_far").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_active_move_operator_coun = Window.partitionBy("is_active", "move_operator_count_dense_rank")
w_is_active_unit_cost_tier_by_2 = Window.partitionBy("is_active", "unit_cost_tier").orderBy(F.col("max_move_count_so_far").desc(), "id").rowsBetween(-6, -1)
w_is_active_unit_cost_tier_by_3 = Window.partitionBy("is_active", "unit_cost_tier").orderBy(F.col("max_move_count_so_far").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_skus
    .filter((F.col("category") == "Toys") | (F.col("sku_code").isNotNull()))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        F.when(F.col("unit_cost") >= 239.3, "large").when(F.col("unit_cost") >= 108.5, "medium").otherwise("small").alias("unit_cost_tier"),
        F.substring(F.trim(F.col("sku_code")), 1, 2).alias("sku_code_prefix"),
        F.when(F.col("unit_cost") >= 277.0, "A").when(F.col("unit_cost") >= 134.3, "B").otherwise("C").alias("unit_cost_tier_2"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count(F.when(F.col("operator") == "op_bee", 1)).alias("move_operator_count"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

sequenced = (
    summarized_rows
    .select(
        "*",
        F.dense_rank().over(w_unit_cost_tier_2_category_by).alias("move_operator_count_dense_rank"),
        F.rank().over(w_category_unit_cost_tier_by_w).alias("weight_kg_rank"),
        F.lag("move_count", 2).over(w_move_count_move_operator_cou).alias("prev2_move_count"),
        F.round(F.col("weight_kg") / F.nullif(F.sum("weight_kg").over(w_unit_cost_tier_2), F.lit(0)), 4).alias("weight_kg_share_of_unit_cost_tier_2"),
        F.max("move_count").over(w_unit_cost_tier_2_category_by_2).alias("max_move_count_so_far"),
        F.round(F.cume_dist().over(w_category_unit_cost_tier_by_w_2), 3).alias("weight_kg_cume_dist"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "sku_code",
        "category",
        "is_active",
        "sku_code_prefix",
        "unit_cost_tier_2",
        "move_operator_count",
        "move_operator_count_dense_rank",
        "weight_kg_rank",
        "prev2_move_count",
        "weight_kg_share_of_unit_cost_tier_2",
        F.sum("move_count").over(w_is_active_unit_cost_tier_by).alias("running_move_count"),
        F.count(F.lit(1)).over(w_is_active_unit_cost_tier_by).alias("running_count"),
        F.round(F.col("unit_cost") - F.avg("unit_cost").over(w_is_active_move_operator_coun), 2).alias("unit_cost_vs_is_active_move_operator_count_dense"),
        F.max("max_move_count_so_far").over(w_is_active_unit_cost_tier_by_2).alias("max_move_count_so_far_max_6_prior"),
        F.nth_value("prev2_move_count", 2).over(w_is_active_unit_cost_tier_by_3).alias("second_prev2_move_count"),
    )
)

result = (
    sequenced_2
    .groupingSets(
        [["weight_kg_rank", "move_operator_count_dense_rank", "prev2_move_count"], ["weight_kg_rank"]],
        "weight_kg_rank", "move_operator_count_dense_rank", "prev2_move_count",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("category").alias("distinct_categorys"),
    )
    .orderBy(
        F.col("weight_kg_rank").asc_nulls_last(),
        F.col("move_operator_count_dense_rank").asc_nulls_last(),
        F.col("prev2_move_count").asc_nulls_last(),
    )
)
