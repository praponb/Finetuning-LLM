from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_operator_move_type_by_moved = Window.partitionBy("operator", "move_type").orderBy("moved_at", "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_unit_price_full = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_move_type_by_qty_ties = Window.partitionBy("sku_id", "move_type").orderBy("qty")

valid_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("move_type").isin("TRANSFER", "ADJUST", "OUTBOUND")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
    )
)

windowed = (
    valid_moves
    .select(
        "*",
        F.min("unit_price").over(w_sku_id_move_type).alias("sku_id_move_type_min_unit_price"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_move_type), 2).alias("unit_price_vs_sku_id_move_type_avg"),
        (F.col("unit_price") - F.lag("unit_price").over(w_operator_move_type_by_moved)).alias("unit_price_change"),
        F.min("qty").over(w_warehouse_id).alias("warehouse_id_min_qty"),
        F.count(F.lit(1)).over(w_sku_id_move_type).alias("rows_in_sku_id_move_type"),
        F.last("moved_at").over(w_all_by_unit_price_full).alias("last_moved_at"),
        F.round(F.avg("unit_price").over(w_warehouse_id), 2).alias("warehouse_id_avg_unit_price"),
        F.round(F.cume_dist().over(w_sku_id_move_type_by_qty_ties), 3).alias("qty_cume_dist"),
        F.size(F.collect_set("operator").over(w_sku_id_move_type)).alias("distinct_operator_in_sku_id_move_type"),
    )
)

labeled = (
    windowed
    .filter(F.col("last_moved_at") < F.lit("2025-02-12 15:00:00").cast("timestamp"))
    .select(
        "*",
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
    )
)

result = (
    labeled
    .select(
        "id",
        "qty",
        "operator",
        "unit_price",
        "move_type_ref",
        "sku_id_move_type_min_unit_price",
        "unit_price_vs_sku_id_move_type_avg",
        "unit_price_change",
        "rows_in_sku_id_move_type",
        "warehouse_id_avg_unit_price",
        "qty_cume_dist",
        "distinct_operator_in_sku_id_move_type",
        "move_type_prefix",
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)
