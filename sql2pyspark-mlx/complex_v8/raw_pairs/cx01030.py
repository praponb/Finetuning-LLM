from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_move_type_by_moved_at = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_moved_at_prior = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(-1, -1)

valid = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("id").cast("string")).alias("operator_ref"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.sum("unit_price").over(w_sku_id_move_type).alias("sku_id_move_type_sum_unit_price"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_move_type), 2).alias("unit_price_vs_sku_id_move_type_avg"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_move_type), F.lit(0)), 4).alias("unit_price_share_of_sku_id_move_type"),
        F.size(F.collect_set("operator").over(w_sku_id_move_type)).alias("distinct_operator_in_sku_id_move_type"),
        F.lead("moved_at").over(w_move_type_by_moved_at).alias("next_moved_at"),
        F.count(F.lit(1)).over(w_sku_id_move_type).alias("rows_in_sku_id_move_type"),
        F.last("moved_at").over(w_all_by_moved_at_full).alias("last_moved_at"),
        F.round(F.avg("unit_price").over(w_move_type_by_moved_at_prior), 2).alias("unit_price_avg_1_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "sku_id_move_type_sum_unit_price",
        "unit_price_vs_sku_id_move_type_avg",
        "unit_price_share_of_sku_id_move_type",
        "distinct_operator_in_sku_id_move_type",
        "next_moved_at",
        "rows_in_sku_id_move_type",
        "last_moved_at",
        "unit_price_avg_1_prior",
    )
    .orderBy(
        F.col("unit_price_share_of_sku_id_move_type").desc(),
        F.col("unit_price").desc(),
    )
)
