from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_all_by_moved_at_running = Window.orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_moves = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

with_windows = (
    scoped_moves
    .select(
        "*",
        F.first("unit_price").over(w_all_by_moved_at_full).alias("first_unit_price"),
        F.count(F.lit(1)).over(w_sku_id_warehouse_id).alias("rows_in_sku_id_warehouse_id"),
        F.last("qty_filled", ignorenulls=True).over(w_all_by_moved_at_running).alias("last_known_qty_filled"),
        F.size(F.collect_set("operator").over(w_sku_id_warehouse_id)).alias("distinct_operator_in_sku_id_warehouse_id"),
    )
)

per_group = (
    with_windows
    .groupBy("warehouse_id", "move_type")
    .agg(
        F.round(F.stddev("qty"), 2).alias("stddev_qty"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.max("first_unit_price").alias("max_first_unit_price_check"),
    )
    .filter(F.col("max_first_unit_price_check") > 161.4)
    .drop("max_first_unit_price_check")
)

with_details_warehouse = (
    per_group.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.warehouse_id",
        "s.move_type",
        "s.stddev_qty",
        "s.move_type_list",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

joined = (
    with_details_warehouse.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.warehouse_id",
        "s.move_type",
        "s.stddev_qty",
        "s.move_type_list",
        "s.warehouse_wh_code",
        "s.warehouse_capacity",
        "s.warehouse_opened_on",
        F.col("w.wh_code").alias("warehouse_wh_code_2"),
    )
)

result = (
    joined
    .select(
        "warehouse_id",
        "move_type",
        "stddev_qty",
        "move_type_list",
        "warehouse_capacity",
        "warehouse_opened_on",
        F.round(F.col("stddev_qty") * 0.25, 2).alias("stddev_qty_adj"),
    )
    .orderBy(F.col("move_type").asc())
)
