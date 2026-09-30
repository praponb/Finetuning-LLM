from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_all_by_qty_moving_6 = Window.orderBy("qty", "id").rowsBetween(-6, Window.currentRow)
w_all_by_qty_prior_2 = Window.orderBy("qty", "id").rowsBetween(-2, -1)
w_move_type_warehouse_id_by_un = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_move_type_by_qty_prio = Window.partitionBy("sku_id", "move_type").orderBy("qty", "id").rowsBetween(-6, -1)
w_move_type_warehouse_id_by_un_2 = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_move_type_by_qty = Window.partitionBy("sku_id", "move_type").orderBy("qty", "id")
w_move_type_sku_id = Window.partitionBy("move_type", "sku_id")
w_warehouse_id_move_type_by_un = Window.partitionBy("warehouse_id", "move_type").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_unit_price_decile = Window.partitionBy("move_type", "unit_price_decile").orderBy("unit_price_max_2_prior", "id")
w_move_type_by_qty = Window.partitionBy("move_type").orderBy("qty", "id")
w_unit_price_decile_sku_id = Window.partitionBy("unit_price_decile", "sku_id")
w_move_type_by_qty_running = Window.partitionBy("move_type").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_moves = (
    df_stock_moves
    .filter(F.col("unit_price") > 200.7)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
    )
)

with_trends = (
    scoped_moves
    .select(
        "*",
        F.max("unit_price").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_max_unit_price"),
        F.round(F.avg("unit_price").over(w_all_by_qty_moving_6), 2).alias("unit_price_avg_7"),
        F.max("unit_price").over(w_all_by_qty_prior_2).alias("unit_price_max_2_prior"),
        F.last("qty", ignorenulls=True).over(w_move_type_warehouse_id_by_un).alias("last_known_qty"),
        F.round(F.avg("unit_price").over(w_sku_id_move_type_by_qty_prio), 2).alias("unit_price_avg_6_prior"),
        F.ntile(10).over(w_move_type_warehouse_id_by_un_2).alias("unit_price_decile"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_qty)).alias("unit_price_change"),
        F.min("unit_price").over(w_warehouse_id).alias("warehouse_id_min_unit_price"),
        F.round(F.cume_dist().over(w_sku_id_move_type_by_qty), 3).alias("qty_cume_dist"),
        F.round(F.avg("qty").over(w_move_type_sku_id), 2).alias("move_type_sku_id_avg_qty"),
        F.min("unit_price").over(w_warehouse_id_move_type_by_un).alias("min_unit_price_so_far"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "move_type_warehouse_id_max_unit_price",
        "unit_price_avg_7",
        "unit_price_max_2_prior",
        "unit_price_decile",
        "unit_price_change",
        "qty_cume_dist",
        "min_unit_price_so_far",
        F.ntile(3).over(w_move_type_unit_price_decile).alias("unit_price_max_2_prior_tercile"),
        F.ntile(10).over(w_move_type_by_qty).alias("qty_decile"),
        F.round(F.col("qty") - F.avg("qty").over(w_unit_price_decile_sku_id), 2).alias("qty_vs_unit_price_decile_sku_id_avg"),
        F.first("warehouse_id_min_unit_price", ignorenulls=True).over(w_move_type_by_qty_running).alias("first_known_warehouse_id_min_unit_price"),
        F.count(F.lit(1)).over(w_unit_price_decile_sku_id).alias("rows_in_unit_price_decile_sku_id"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price_avg_7",
        "unit_price_max_2_prior",
        "unit_price_change",
        "min_unit_price_so_far",
        "unit_price_max_2_prior_tercile",
        "qty_decile",
        "qty_vs_unit_price_decile_sku_id_avg",
        "first_known_warehouse_id_min_unit_price",
        "rows_in_unit_price_decile_sku_id",
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
    )
    .orderBy(F.col("qty_decile").desc(), "unit_price_max_2_prior")
)
