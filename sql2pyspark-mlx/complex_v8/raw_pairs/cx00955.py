from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_qty_moving_6 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_qty_full = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_sku_id_operator_by_qty_full = Window.partitionBy("sku_id", "operator").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month_by_second_mov = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy("second_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month_by_second_mov_2 = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy("second_moved_at")
w_distinct_sku_id_in_warehouse = Window.partitionBy("distinct_sku_id_in_warehouse_id", "warehouse_id")

recent_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
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

windowed = (
    recent_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id), 2).alias("qty_vs_warehouse_id_avg"),
        F.max("qty").over(w_warehouse_id).alias("warehouse_id_max_qty"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
        F.sum("qty").over(w_all_by_qty_moving_6).alias("qty_sum_7"),
        F.nth_value("moved_at", 2).over(w_all_by_qty_full).alias("second_moved_at"),
        F.sum("unit_price").over(w_warehouse_id).alias("warehouse_id_sum_unit_price"),
        F.last("qty").over(w_all_by_qty_full).alias("last_qty"),
        F.sum("qty").over(w_warehouse_id).alias("warehouse_id_sum_qty"),
        F.row_number().over(w_all_by_qty).alias("seq_no"),
        F.nth_value("qty", 2).over(w_sku_id_operator_by_qty_full).alias("second_qty"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "warehouse_id_max_qty",
        "distinct_sku_id_in_warehouse_id",
        "qty_sum_7",
        "warehouse_id_sum_unit_price",
        "seq_no",
        "second_qty",
        F.last("qty_sum_7").over(w_moved_at_month_by_second_mov).alias("last_qty_sum_7"),
        F.round(F.percent_rank().over(w_moved_at_month_by_second_mov_2), 3).alias("second_moved_at_percent_rank"),
        F.size(F.collect_set("sku_id").over(w_distinct_sku_id_in_warehouse)).alias("distinct_sku_id_in_distinct_sku_id_in_warehouse_"),
        F.round(F.col("warehouse_id_max_qty") / F.nullif(F.sum("warehouse_id_max_qty").over(w_distinct_sku_id_in_warehouse), F.lit(0)), 4).alias("warehouse_id_max_qty_share_of_distinct_sku_id_in"),
        F.nth_value("warehouse_id_max_qty", 2).over(w_moved_at_month_by_second_mov).alias("second_warehouse_id_max_qty"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "warehouse_id_max_qty",
        "distinct_sku_id_in_warehouse_id",
        "qty_sum_7",
        "seq_no",
        "last_qty_sum_7",
        "second_moved_at_percent_rank",
        "distinct_sku_id_in_distinct_sku_id_in_warehouse_",
        "warehouse_id_max_qty_share_of_distinct_sku_id_in",
        "second_warehouse_id_max_qty",
        F.abs(F.col("qty_sum_7") - 676).alias("qty_sum_7_distance"),
        (F.col("qty_sum_7") + F.col("seq_no")).alias("qty_sum_7_plus_seq_no"),
    )
    .orderBy(F.col("distinct_sku_id_in_warehouse_id").asc_nulls_last(), "sku_id")
)
