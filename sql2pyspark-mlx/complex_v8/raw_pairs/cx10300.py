from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_move_type_by_unit_p = Window.partitionBy("operator", "move_type").orderBy("unit_price_per_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_running = Window.orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_move_type = Window.partitionBy("operator", "move_type")
w_all_by_qty = Window.orderBy(F.col("qty").desc_nulls_first(), "id")
w_warehouse_id_move_type_by_ro = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("rows_in_operator_move_type").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_move_type_warehouse_id_by_ma = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("max_unit_price_so_far_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_warehouse_id_by_ma_2 = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("max_unit_price_so_far_filled").desc(), "id")

cleaned = (
    df_stock_moves
    .filter(F.col("sku_id") >= 15)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.sum("qty").over(w_operator_move_type_by_unit_p).alias("running_qty"),
        F.max("unit_price").over(w_all_by_qty_running).alias("max_unit_price_so_far"),
        F.round(F.avg("unit_price").over(w_operator_move_type), 2).alias("operator_move_type_avg_unit_price"),
        F.count(F.lit(1)).over(w_operator_move_type).alias("rows_in_operator_move_type"),
        F.ntile(10).over(w_all_by_qty).alias("qty_decile"),
        F.sum("unit_price_per_qty").over(w_operator_move_type).alias("operator_move_type_sum_unit_price_per_qty"),
    )
)

with_details = (
    with_trends.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.city") == "Singapore"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_per_qty",
        "s.max_unit_price_so_far",
        "s.rows_in_operator_move_type",
        "s.qty_decile",
        "s.operator_move_type_sum_unit_price_per_qty",
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

with_details_2 = (
    with_details.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.opened_on") >= F.lit("2024-11-25").cast("date"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_per_qty",
        "s.max_unit_price_so_far",
        "s.rows_in_operator_move_type",
        "s.warehouse_wh_code",
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

tagged = (
    with_details_2
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "unit_price_per_qty",
        "rows_in_operator_move_type",
        "warehouse_wh_code",
        F.coalesce(F.col("max_unit_price_so_far"), F.lit(0.0)).alias("max_unit_price_so_far_filled"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.nth_value("unit_price", 2).over(w_warehouse_id_move_type_by_ro).alias("second_unit_price"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_move_type), 2).alias("unit_price_vs_warehouse_id_move_type_avg"),
        F.count(F.lit(1)).over(w_warehouse_id_move_type).alias("rows_in_warehouse_id_move_type"),
        F.last("rows_in_operator_move_type", ignorenulls=True).over(w_move_type_warehouse_id_by_ma).alias("last_known_rows_in_operator_move_type"),
        F.ntile(3).over(w_move_type_warehouse_id_by_ma_2).alias("max_unit_price_so_far_filled_tercile"),
    )
)

tagged_2 = (
    sequenced
    .select(
        "*",
        F.date_trunc("quarter", F.col("moved_at")).alias("moved_at_quarter"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

result = (
    tagged_2
    .select(
        "id",
        "qty",
        "unit_price",
        "unit_price_per_qty",
        "rows_in_operator_move_type",
        "max_unit_price_so_far_filled",
        "unit_price_vs_warehouse_id_move_type_avg",
        "last_known_rows_in_operator_move_type",
        "max_unit_price_so_far_filled_tercile",
        "moved_at_quarter",
        "qty_filled",
        F.concat_ws("|", F.col("warehouse_wh_code"), F.col("move_type")).alias("warehouse_wh_code_move_type_label"),
    )
    .orderBy(
        F.col("moved_at_quarter").desc(),
        F.col("unit_price_per_qty").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
