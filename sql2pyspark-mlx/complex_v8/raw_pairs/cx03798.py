from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_all_by_qty_full = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_distance = Window.orderBy("unit_price_distance", "id")
w_all_by_qty_prior_6 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-6, -1)
w_sku_id_by_unit_price_distanc = Window.partitionBy("sku_id").orderBy("unit_price_distance")
w_sku_id_by_unit_price_distanc_2 = Window.partitionBy("sku_id").orderBy("unit_price_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_warehouse_id_move_typ = Window.orderBy(F.col("warehouse_id_move_type_max_unit_price_distance").desc(), "id").rowsBetween(-6, -1)
w_all_by_warehouse_id_move_typ_2 = Window.orderBy(F.col("warehouse_id_move_type_max_unit_price_distance").desc(), "id")
w_rn_warehouse_id_by_last_move = Window.partitionBy("rn", "warehouse_id").orderBy("last_moved_at", "id")

scoped_moves = (
    df_stock_moves
    .filter((F.col("unit_price") >= 215.4) & (F.col("operator") == "op_bee"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.abs(F.col("unit_price") - 264.8).alias("unit_price_distance"),
    )
)

windowed = (
    scoped_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price_distance",
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.max("unit_price_distance").over(w_warehouse_id_move_type).alias("warehouse_id_move_type_max_unit_price_distance"),
        F.round(F.col("unit_price_distance") - F.avg("unit_price_distance").over(w_sku_id), 2).alias("unit_price_distance_vs_sku_id_avg"),
        F.nth_value("moved_at", 2).over(w_all_by_qty_full).alias("second_moved_at"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id_move_type), 2).alias("qty_vs_warehouse_id_move_type_avg"),
        F.row_number().over(w_all_by_unit_price_distance).alias("rn"),
        F.max("unit_price_distance").over(w_all_by_qty_prior_6).alias("unit_price_distance_max_6_prior"),
        F.round(F.cume_dist().over(w_sku_id_by_unit_price_distanc), 3).alias("unit_price_distance_cume_dist"),
        F.last("moved_at").over(w_sku_id_by_unit_price_distanc_2).alias("last_moved_at"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.round(F.avg("sku_id_avg_qty").over(w_all_by_warehouse_id_move_typ), 2).alias("sku_id_avg_qty_avg_6_prior"),
        F.min("rn").over(w_sku_id).alias("sku_id_min_rn"),
        F.max("unit_price_distance").over(w_sku_id).alias("sku_id_max_unit_price_distance"),
        F.ntile(3).over(w_all_by_warehouse_id_move_typ_2).alias("warehouse_id_move_type_max_unit_price_distance_t"),
        F.ntile(5).over(w_rn_warehouse_id_by_last_move).alias("last_moved_at_quintile"),
    )
)

result = (
    sequenced
    .filter(F.col("unit_price_distance") < 275.0)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price_distance",
        "sku_id_avg_qty",
        "warehouse_id_move_type_max_unit_price_distance",
        "unit_price_distance_vs_sku_id_avg",
        "second_moved_at",
        "unit_price_distance_cume_dist",
        "last_moved_at",
        "sku_id_avg_qty_avg_6_prior",
        "sku_id_min_rn",
        "sku_id_max_unit_price_distance",
        "warehouse_id_move_type_max_unit_price_distance_t",
        "last_moved_at_quintile",
        F.concat(F.col("move_type"), F.lit("#"), F.col("last_moved_at_quintile").cast("string")).alias("move_type_ref"),
    )
    .orderBy(F.col("sku_id").asc_nulls_last())
)
