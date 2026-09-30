from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_operator_move_type = Window.partitionBy("operator", "move_type")
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_sku_id_operator_by_moved_at = Window.partitionBy("sku_id", "operator").orderBy("moved_at", "id").rowsBetween(-1, -1)
w_move_type_by_qty_running = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

ranked = (
    base_moves
    .select(
        "*",
        F.count(F.lit(1)).over(w_warehouse_id_move_type).alias("rows_in_warehouse_id_move_type"),
        F.count(F.lit(1)).over(w_operator_move_type).alias("rows_in_operator_move_type"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type_operator), 2).alias("unit_price_vs_move_type_operator_avg"),
        F.max("unit_price").over(w_sku_id_operator_by_moved_at).alias("unit_price_max_1_prior"),
        F.round(F.col("qty") - F.avg("qty").over(w_move_type_operator), 2).alias("qty_vs_move_type_operator_avg"),
        F.last("unit_price", ignorenulls=True).over(w_move_type_by_qty_running).alias("last_known_unit_price"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id_move_type), 2).alias("qty_vs_warehouse_id_move_type_avg"),
        F.round(F.percent_rank().over(w_all_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
        F.rank().over(w_all_by_moved_at_ties).alias("moved_at_rank"),
        F.last("moved_at").over(w_all_by_moved_at_full).alias("last_moved_at"),
    )
)

top_per_group = ranked.filter((F.col("moved_at_rank") <= 5) & (F.col("operator") == "op_bee"))

result = (
    top_per_group
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "rows_in_warehouse_id_move_type",
        "rows_in_operator_move_type",
        "unit_price_vs_move_type_operator_avg",
        "unit_price_max_1_prior",
        "qty_vs_move_type_operator_avg",
        "last_known_unit_price",
        "qty_vs_warehouse_id_move_type_avg",
        "moved_at_percent_rank",
        "moved_at_rank",
        "last_moved_at",
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
    )
    .orderBy("moved_at_plus_15m")
)
