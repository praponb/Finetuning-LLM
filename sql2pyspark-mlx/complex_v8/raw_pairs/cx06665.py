from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_move_type_by_qty = Window.partitionBy("operator", "move_type").orderBy(F.col("qty").asc_nulls_last(), "id")
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_qty = Window.orderBy("qty", "id")
w_operator_move_type_by_qty_pr = Window.partitionBy("operator", "move_type").orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_move_type_by_qty = Window.partitionBy("move_type").orderBy("qty", "id")
w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_move_type_by_qty_fu = Window.partitionBy("operator", "move_type").orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_operator_move_type_by_qty_ti = Window.partitionBy("operator", "move_type").orderBy(F.col("qty").asc_nulls_last())

cleaned = (
    df_stock_moves
    .filter(F.col("qty") <= 445)
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

sequenced = (
    cleaned
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") - F.lag("qty").over(w_operator_move_type_by_qty)).alias("qty_change"),
        F.size(F.collect_set("move_type").over(w_operator_warehouse_id)).alias("distinct_move_type_in_operator_warehouse_id"),
        F.ntile(4).over(w_all_by_unit_price).alias("unit_price_quartile"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_qty)).alias("unit_price_change"),
        F.round(F.avg("unit_price").over(w_operator_move_type_by_qty_pr), 2).alias("unit_price_avg_1_prior"),
        F.ntile(3).over(w_move_type_by_qty).alias("qty_tercile"),
        F.sum("qty").over(w_all_by_unit_price_running).alias("running_qty"),
        F.row_number().over(w_all_by_unit_price).alias("seq_no"),
        F.nth_value("qty", 2).over(w_operator_move_type_by_qty_fu).alias("second_qty"),
        F.count(F.lit(1)).over(w_operator_warehouse_id).alias("rows_in_operator_warehouse_id"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_operator_warehouse_id), 2).alias("unit_price_vs_operator_warehouse_id_avg"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_warehouse_id"),
        F.round(F.col("qty") - F.avg("qty").over(w_operator_warehouse_id), 2).alias("qty_vs_operator_warehouse_id_avg"),
        F.round(F.cume_dist().over(w_operator_move_type_by_qty_ti), 3).alias("qty_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "qty_change",
        "unit_price_quartile",
        "unit_price_change",
        "unit_price_avg_1_prior",
        "qty_tercile",
        "second_qty",
        "rows_in_operator_warehouse_id",
        "unit_price_vs_operator_warehouse_id_avg",
        "unit_price_share_of_warehouse_id",
        "qty_vs_operator_warehouse_id_avg",
        "qty_cume_dist",
    )
)
