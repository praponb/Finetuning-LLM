from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_hour = Window.partitionBy("moved_at_hour")
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_all_by_unit_price_full = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty_moving_1 = Window.orderBy("qty", "id").rowsBetween(-1, Window.currentRow)
w_warehouse_id_by_moved_at_tie = Window.partitionBy("warehouse_id").orderBy("moved_at")
w_all_by_qty_ties = Window.orderBy("qty")
w_all_by_qty_full = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_all_by_distinct_warehouse_id = Window.orderBy(F.col("distinct_warehouse_id_in_move_type_operator").desc())
w_operator_by_second_qty_full = Window.partitionBy("operator").orderBy("second_qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_moves = (
    df_stock_moves
    .filter((F.col("qty") <= 271) & (F.col("move_type") == "ADJUST"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
    )
)

metrics = (
    recent_moves
    .select(
        "*",
        F.size(F.collect_set("sku_id").over(w_moved_at_hour)).alias("distinct_sku_id_in_moved_at_hour"),
        F.round(F.percent_rank().over(w_warehouse_id_by_moved_at), 3).alias("moved_at_percent_rank"),
        F.min("unit_price").over(w_move_type_operator).alias("move_type_operator_min_unit_price"),
        F.nth_value("qty", 2).over(w_all_by_unit_price_full).alias("second_qty"),
        F.max("unit_price").over(w_all_by_qty_moving_1).alias("unit_price_max_2"),
        F.size(F.collect_set("warehouse_id").over(w_move_type_operator)).alias("distinct_warehouse_id_in_move_type_operator"),
        F.round(F.percent_rank().over(w_warehouse_id_by_moved_at_tie), 3).alias("moved_at_percent_rank_2"),
        F.rank().over(w_all_by_qty_ties).alias("qty_rank"),
        F.first("moved_at_hour").over(w_all_by_qty_full).alias("first_moved_at_hour"),
    )
)

top_per_group = (
    metrics
    .filter((F.col("qty_rank") <= 5) & (F.col("qty").between(99, 288)))
    .drop("qty_rank")
)

metrics_2 = (
    top_per_group
    .select(
        "*",
        F.count(F.lit(1)).over(w_operator_warehouse_id).alias("rows_in_operator_warehouse_id"),
        F.dense_rank().over(w_all_by_distinct_warehouse_id).alias("distinct_warehouse_id_in_move_type_operator_dens"),
        F.first("second_qty").over(w_operator_by_second_qty_full).alias("first_second_qty"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "moved_at_hour",
        "distinct_sku_id_in_moved_at_hour",
        "moved_at_percent_rank",
        "move_type_operator_min_unit_price",
        "second_qty",
        "unit_price_max_2",
        "distinct_warehouse_id_in_move_type_operator",
        "first_moved_at_hour",
        "first_second_qty",
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
    )
    .orderBy(
        F.col("distinct_warehouse_id_in_move_type_operator").desc_nulls_first(),
        F.col("sku_id").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
