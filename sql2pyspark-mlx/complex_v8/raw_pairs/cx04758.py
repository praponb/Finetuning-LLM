from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_id_by_moved_at = Window.partitionBy("operator", "sku_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_qty = Window.partitionBy("sku_id").orderBy("qty", "id")
w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())
w_operator = Window.partitionBy("operator")
w_warehouse_id_move_type_by_wa = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("warehouse_capacity").desc(), "id")
w_sku_id_by_qty_ties = Window.partitionBy("sku_id").orderBy("qty")
w_all_by_qty_full = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at", "id").rowsBetween(-6, Window.currentRow)
w_cnt_warehouse_id = Window.partitionBy("cnt", "warehouse_id")
w_all_by_cnt_ties = Window.orderBy("cnt")

cleaned_moves = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-02-12 14:00:00").cast("timestamp")) & (F.col("qty") <= 314))
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

with_parent_warehouse = (
    cleaned_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

with_trends = (
    with_parent_warehouse
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "warehouse_capacity",
        "warehouse_wh_code",
        F.first("warehouse_capacity", ignorenulls=True).over(w_operator_sku_id_by_moved_at).alias("first_known_warehouse_capacity"),
        (F.col("warehouse_capacity") - F.lag("warehouse_capacity").over(w_sku_id_by_qty)).alias("warehouse_capacity_change"),
        F.round(F.cume_dist().over(w_all_by_qty_ties), 3).alias("qty_cume_dist"),
        F.size(F.collect_set("sku_id").over(w_operator)).alias("distinct_sku_id_in_operator"),
        F.round(F.percent_rank().over(w_all_by_qty_ties), 3).alias("qty_percent_rank"),
        F.sum("unit_price").over(w_operator).alias("operator_sum_unit_price"),
        F.row_number().over(w_warehouse_id_move_type_by_wa).alias("rn"),
        F.round(F.percent_rank().over(w_sku_id_by_qty_ties), 3).alias("qty_percent_rank_2"),
        F.last("warehouse_capacity").over(w_all_by_qty_full).alias("last_warehouse_capacity"),
        F.min("unit_price").over(w_operator).alias("operator_min_unit_price"),
        F.max("qty").over(w_warehouse_id_move_type_by_mo).alias("qty_max_7"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
    )
)

totals = (
    with_trends
    .filter(F.col("last_warehouse_capacity") <= 11005)
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("qty_percent_rank").alias("total_qty_percent_rank"),
        F.round(F.stddev("qty_max_7"), 2).alias("stddev_qty_max_7"),
        F.min("last_warehouse_capacity").alias("min_last_warehouse_capacity"),
    )
    .filter(F.col("cnt") >= 2)
)

metrics = (
    totals
    .select(
        "warehouse_id",
        "total_qty_percent_rank",
        "stddev_qty_max_7",
        "min_last_warehouse_capacity",
        F.round(F.avg("total_qty_percent_rank").over(w_cnt_warehouse_id), 2).alias("cnt_warehouse_id_avg_total_qty_percent_rank"),
        F.rank().over(w_all_by_cnt_ties).alias("cnt_rank"),
    )
)

result = (
    metrics
    .select(
        "warehouse_id",
        "total_qty_percent_rank",
        "stddev_qty_max_7",
        "cnt_warehouse_id_avg_total_qty_percent_rank",
    )
    .orderBy("cnt_warehouse_id_avg_total_qty_percent_rank")
)
