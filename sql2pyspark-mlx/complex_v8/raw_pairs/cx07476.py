from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_all_by_unit_price_running_2 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price_moving_6 = Window.orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w_sku_id_by_unit_price_moving = Window.partitionBy("sku_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy(F.col("unit_price").desc(), "id")

filtered_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") >= 5) & (F.col("sku_id") <= 6))
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
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

ranked = (
    with_parent_warehouse
    .select(
        "*",
        F.min("unit_price").over(w_all_by_unit_price_running).alias("min_unit_price_so_far"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_move_type), 2).alias("unit_price_vs_sku_id_move_type_avg"),
        F.count(F.lit(1)).over(w_sku_id_move_type).alias("rows_in_sku_id_move_type"),
        F.min("unit_price").over(w_all_by_unit_price_running_2).alias("min_unit_price_so_far_2"),
        F.round(F.avg("unit_price").over(w_all_by_unit_price_moving_6), 2).alias("unit_price_avg_7"),
        F.round(F.avg("unit_price").over(w_sku_id_move_type), 2).alias("sku_id_move_type_avg_unit_price"),
        F.sum("unit_price").over(w_sku_id_by_unit_price_moving).alias("unit_price_sum_3"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_unit_price)).alias("unit_price_change"),
        F.round(F.cume_dist().over(w_all_by_unit_price_ties), 3).alias("unit_price_cume_dist"),
        (F.col("unit_price") - F.lag("unit_price").over(w_sku_id_by_unit_price)).alias("unit_price_change_2"),
    )
)

tagged = (
    ranked
    .select(
        "*",
        F.concat(F.col("warehouse_wh_code"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("warehouse_wh_code_ref"),
    )
)

result = (
    tagged
    .filter(F.col("moved_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp"))
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "warehouse_city",
        "warehouse_wh_code",
        "min_unit_price_so_far",
        "unit_price_vs_sku_id_move_type_avg",
        "unit_price_avg_7",
        "unit_price_sum_3",
        "unit_price_cume_dist",
        "unit_price_change_2",
        "warehouse_wh_code_ref",
    )
)
