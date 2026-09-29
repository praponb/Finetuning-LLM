from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_operator_move_type_by_qty = Window.partitionBy("operator", "move_type").orderBy(F.col("qty").desc_nulls_first(), "id")
w_move_type_by_unit_price_prio = Window.partitionBy("move_type").orderBy("unit_price", "id").rowsBetween(-2, -1)
w_operator_moved_at_month_by_u = Window.partitionBy("operator", F.month("moved_at")).orderBy("unit_price", "id").rowsBetween(-6, -1)
w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id = Window.partitionBy("sku_id")
w_all_by_unit_price_ties = Window.orderBy("unit_price")
w_move_type_by_unit_price_movi = Window.partitionBy("move_type").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)

scoped_moves = (
    df_stock_moves
    .filter((F.col("unit_price") <= 367.6) & (F.col("sku_id") <= 4))
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
    scoped_moves
    .select(
        "*",
        F.max("unit_price").over(w_warehouse_id).alias("warehouse_id_max_unit_price"),
        F.dense_rank().over(w_operator_move_type_by_qty).alias("qty_dense_rank"),
        F.round(F.avg("unit_price").over(w_move_type_by_unit_price_prio), 2).alias("unit_price_avg_2_prior"),
        F.round(F.avg("unit_price").over(w_operator_moved_at_month_by_u), 2).alias("unit_price_avg_6_prior"),
        F.last("qty", ignorenulls=True).over(w_all_by_unit_price_running).alias("last_known_qty"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.round(F.cume_dist().over(w_all_by_unit_price_ties), 3).alias("unit_price_cume_dist"),
        F.max("qty").over(w_move_type_by_unit_price_prio).alias("qty_max_2_prior"),
        F.round(F.avg("qty").over(w_move_type_by_unit_price_movi), 2).alias("qty_avg_3"),
    )
)

result = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "warehouse_id_max_unit_price",
        "unit_price_avg_6_prior",
        "last_known_qty",
        "rows_in_sku_id",
        "unit_price_cume_dist",
        "qty_avg_3",
        F.coalesce(F.col("rows_in_sku_id"), F.lit(0)).alias("rows_in_sku_id_filled"),
    )
)
