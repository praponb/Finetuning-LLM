from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_sku_id = Window.partitionBy("sku_id")
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_sku_id_by_unit_price_full = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_operator_warehouse_id_by_qty = Window.partitionBy("operator", "warehouse_id").orderBy("qty", "id")
w_all_by_moved_at_running = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_unit_price_running = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_moves = (
    df_stock_moves
    .filter((F.col("move_type").isin("ADJUST", "OUTBOUND", "INBOUND")) & (F.col("warehouse_id") >= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

sequenced = (
    recent_moves
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator_warehouse_id), F.lit(0)), 4).alias("qty_share_of_operator_warehouse_id"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
        F.round(F.avg("qty").over(w_operator_warehouse_id), 2).alias("operator_warehouse_id_avg_qty"),
        F.row_number().over(w_sku_id_by_unit_price).alias("seq_no"),
        F.sum("unit_price").over(w_sku_id).alias("sku_id_sum_unit_price"),
        F.nth_value("moved_at", 2).over(w_sku_id_by_unit_price_full).alias("second_moved_at"),
        F.sum("unit_price").over(w_operator_warehouse_id).alias("operator_warehouse_id_sum_unit_price"),
        F.nth_value("qty", 2).over(w_all_by_moved_at_full).alias("second_qty"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.row_number().over(w_all_by_unit_price).alias("rn"),
        (F.col("unit_price") - F.lag("unit_price").over(w_operator_warehouse_id_by_qty)).alias("unit_price_change"),
        F.count(F.lit(1)).over(w_all_by_moved_at_running).alias("running_count"),
        F.sum("qty").over(w_sku_id_by_unit_price_running).alias("running_qty"),
    )
)

result = (
    sequenced
    .filter(F.col("operator_warehouse_id_sum_unit_price") < 3000.4)
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "qty_share_of_operator_warehouse_id",
        "distinct_warehouse_id_in_sku_id",
        "operator_warehouse_id_avg_qty",
        "seq_no",
        "operator_warehouse_id_sum_unit_price",
        "sku_id_avg_qty",
        "rn",
        "unit_price_change",
        "running_count",
        "running_qty",
        F.coalesce(F.col("rn"), F.lit(0)).alias("rn_filled"),
    )
    .orderBy(F.col("running_qty").asc(), F.col("id").desc())
)
