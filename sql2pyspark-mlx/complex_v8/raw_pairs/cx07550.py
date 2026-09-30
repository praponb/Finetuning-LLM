from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_sku_id_operator_by_warehouse = Window.partitionBy("sku_id", "operator").orderBy("warehouse_opened_on", "id")
w_warehouse_city_by_qty_moving = Window.partitionBy("warehouse_city").orderBy("qty", "id").rowsBetween(-6, Window.currentRow)
w_warehouse_city_by_qty = Window.partitionBy("warehouse_city").orderBy("qty", "id")
w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_warehouse_city_moved_at_year = Window.partitionBy("warehouse_city", F.year("moved_at")).orderBy("warehouse_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_operator_by_warehouse_2 = Window.partitionBy("sku_id", "operator").orderBy("warehouse_opened_on", "id").rowsBetween(-1, Window.currentRow)
w_warehouse_city_by_qty_moving_2 = Window.partitionBy("warehouse_city").orderBy("qty", "id").rowsBetween(-1, Window.currentRow)
w_operator_by_move_type_avg_le = Window.partitionBy("operator").orderBy(F.col("move_type_avg_least_qty_unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_sku_weight_kg_moving = Window.orderBy(F.col("sku_weight_kg").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_warehouse_city = Window.partitionBy("warehouse_city")

recent_moves = (
    df_stock_moves
    .filter(F.col("unit_price") < 399.6)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

joined_warehouse = (
    recent_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_trends = (
    joined_warehouse
    .select(
        "*",
        F.round(F.avg("least_qty_unit_price").over(w_move_type), 2).alias("move_type_avg_least_qty_unit_price"),
        F.row_number().over(w_sku_id_operator_by_warehouse).alias("rn"),
        F.round(F.avg("least_qty_unit_price").over(w_warehouse_city_by_qty_moving), 2).alias("least_qty_unit_price_avg_7"),
        F.row_number().over(w_warehouse_city_by_qty).alias("seq_no"),
        F.round(F.avg("least_qty_unit_price").over(w_operator_sku_id), 2).alias("operator_sku_id_avg_least_qty_unit_price"),
        F.nth_value("warehouse_opened_on", 2).over(w_warehouse_city_moved_at_year).alias("second_warehouse_opened_on"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.max("qty").over(w_sku_id_operator_by_warehouse_2).alias("qty_max_2"),
        F.round(F.avg("qty").over(w_warehouse_city_by_qty_moving_2), 2).alias("qty_avg_2"),
        F.round(F.col("qty") - F.avg("qty").over(w_operator_sku_id), 2).alias("qty_vs_operator_sku_id_avg"),
    )
)

windowed = (
    with_trends
    .select(
        "*",
        F.last("least_qty_unit_price_avg_7", ignorenulls=True).over(w_operator_by_move_type_avg_le).alias("last_known_least_qty_unit_price_avg_7"),
        F.max("qty_vs_operator_sku_id_avg").over(w_all_by_sku_weight_kg_moving).alias("qty_vs_operator_sku_id_avg_max_4"),
        F.max("least_qty_unit_price_avg_7").over(w_warehouse_city).alias("warehouse_city_max_least_qty_unit_price_avg_7"),
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
        "moved_at",
        "operator",
        "least_qty_unit_price",
        "warehouse_opened_on",
        "warehouse_city",
        "move_type_avg_least_qty_unit_price",
        "rn",
        "least_qty_unit_price_avg_7",
        "seq_no",
        "operator_sku_id_avg_least_qty_unit_price",
        "rows_in_move_type",
        "qty_max_2",
        "last_known_least_qty_unit_price_avg_7",
        "qty_vs_operator_sku_id_avg_max_4",
        "warehouse_city_max_least_qty_unit_price_avg_7",
    )
)
