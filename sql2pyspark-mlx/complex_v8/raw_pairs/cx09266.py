from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_is_active_move_type_by_s = Window.partitionBy("sku_is_active", "move_type").orderBy(F.col("sku_weight_kg").desc(), "id")
w_operator_warehouse_id_by_mov = Window.partitionBy("operator", "warehouse_id").orderBy("moved_at", "id")
w_sku_is_active_move_type_by_s_2 = Window.partitionBy("sku_is_active", "move_type").orderBy(F.col("sku_weight_kg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_is_active_move_type_by_s_3 = Window.partitionBy("sku_is_active", "move_type").orderBy(F.col("sku_weight_kg").desc(), "id").rowsBetween(-1, -1)
w_operator_by_moved_at = Window.partitionBy("operator").orderBy("moved_at", "id")
w_move_type = Window.partitionBy("move_type")
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")

base_moves = (
    df_stock_moves
    .filter(F.col("qty") > 116)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("operator_ref"),
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
    )
)

combined_details = (
    base_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.category") == "Toys")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.move_type_prefix",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

with_windows = (
    combined_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "move_type_prefix",
        "sku_category",
        "sku_weight_kg",
        "sku_is_active",
        F.row_number().over(w_sku_is_active_move_type_by_s).alias("seq_no"),
        F.row_number().over(w_operator_warehouse_id_by_mov).alias("seq_no_2"),
        F.nth_value("sku_weight_kg", 2).over(w_sku_is_active_move_type_by_s_2).alias("second_sku_weight_kg"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_sku_is_active_move_type_by_s))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.first("moved_at").over(w_sku_is_active_move_type_by_s_2).alias("first_moved_at"),
        F.sum("sku_weight_kg").over(w_sku_is_active_move_type_by_s_3).alias("sku_weight_kg_sum_1_prior"),
        (F.col("sku_weight_kg") - F.lag("sku_weight_kg").over(w_operator_by_moved_at)).alias("sku_weight_kg_change"),
        F.nth_value("moved_at", 2).over(w_sku_is_active_move_type_by_s_2).alias("second_moved_at"),
        F.min("sku_weight_kg").over(w_move_type).alias("move_type_min_sku_weight_kg"),
        F.round(F.col("sku_weight_kg") / F.nullif(F.sum("sku_weight_kg").over(w_sku_id_move_type), F.lit(0)), 4).alias("sku_weight_kg_share_of_sku_id_move_type"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("seq_no_2") == 1) & (F.col("warehouse_id") <= 4))
    .drop("seq_no_2")
)

rollup_base = (
    top_per_group
    .groupBy("sku_id")
    .agg(
        F.sum("sku_weight_kg_change").alias("total_sku_weight_kg_change"),
        F.count(F.when(F.col("sku_category").isin("Toys", "Food", "Beauty"), 1)).alias("n_sku_category_in_toys_food_be"),
        F.count("*").alias("cnt"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.round(F.sum("move_type_min_sku_weight_kg"), 2).alias("total_move_type_min_sku_weight_kg"),
    )
)

result = (
    rollup_base
    .select(
        "sku_id",
        "total_sku_weight_kg_change",
        "n_sku_category_in_toys_food_be",
        "distinct_operators",
        "distinct_sku_ids",
        "total_move_type_min_sku_weight_kg",
    )
    .orderBy(
        "total_move_type_min_sku_weight_kg",
        F.col("distinct_operators").desc(),
        "sku_id",
    )
    .limit(20)
)
