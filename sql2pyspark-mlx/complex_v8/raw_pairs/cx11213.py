from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_sku_category_by_sku_weight_k = Window.partitionBy("sku_category").orderBy("sku_weight_kg", "id")
w_all_by_sku_weight_kg_ties = Window.orderBy(F.col("sku_weight_kg").desc())
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_sku_weight_kg = Window.orderBy(F.col("sku_weight_kg").desc(), "id")
w_warehouse_id_by_sku_weight_k = Window.partitionBy("warehouse_id").orderBy(F.col("sku_weight_kg").desc())

recent = (
    df_stock_moves
    .filter((F.col("operator") != "op_chan") & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws("|", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

joined = (
    recent.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.ntile(5).over(w_sku_category_by_sku_weight_k).alias("sku_weight_kg_quintile"),
        F.dense_rank().over(w_all_by_sku_weight_kg_ties).alias("sku_weight_kg_dense_rank"),
        F.round(F.col("sku_weight_kg") - F.avg("sku_weight_kg").over(w_warehouse_id), 2).alias("sku_weight_kg_vs_warehouse_id_avg"),
        F.lead("sku_unit_cost").over(w_all_by_sku_weight_kg).alias("next_sku_unit_cost"),
        F.size(F.collect_set("sku_category").over(w_move_type)).alias("distinct_sku_category_in_move_type"),
        F.round(F.avg("sku_unit_cost").over(w_move_type), 2).alias("move_type_avg_sku_unit_cost"),
        F.round(F.avg("sku_weight_kg").over(w_move_type), 2).alias("move_type_avg_sku_weight_kg"),
        F.round(F.col("sku_weight_kg") / F.nullif(F.sum("sku_weight_kg").over(w_move_type), F.lit(0)), 4).alias("sku_weight_kg_share_of_move_type"),
        F.ntile(3).over(w_all_by_sku_weight_kg).alias("sku_weight_kg_tercile"),
        F.round(F.cume_dist().over(w_warehouse_id_by_sku_weight_k), 3).alias("sku_weight_kg_cume_dist"),
        F.round(F.col("sku_unit_cost") - F.avg("sku_unit_cost").over(w_warehouse_id), 2).alias("sku_unit_cost_vs_warehouse_id_avg"),
    )
)

top_per_group = with_windows.filter(F.col("sku_weight_kg_dense_rank") == 1)

result = (
    top_per_group
    .filter(F.col("distinct_sku_category_in_move_type") <= 8)
    .select(
        "id",
        "sku_id",
        "operator",
        "sku_unit_cost",
        "sku_category",
        "sku_weight_kg",
        "rows_in_move_type",
        "sku_weight_kg_quintile",
        "sku_weight_kg_dense_rank",
        "next_sku_unit_cost",
        "move_type_avg_sku_weight_kg",
        "sku_weight_kg_share_of_move_type",
        "sku_weight_kg_cume_dist",
        "sku_unit_cost_vs_warehouse_id_avg",
        F.round(F.col("sku_weight_kg_tercile") / F.nullif(F.col("sku_unit_cost"), F.lit(0)), 3).alias("sku_weight_kg_tercile_per_sku_unit_cost"),
        F.abs(F.col("sku_weight_kg_quintile") - 4).alias("sku_weight_kg_quintile_distance"),
    )
    .orderBy("sku_unit_cost", "id")
    .limit(5)
)
