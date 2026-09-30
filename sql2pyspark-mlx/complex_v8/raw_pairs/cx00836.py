from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy("moved_at", "id")
w_operator_sku_id_by_unit_pric = Window.partitionBy("operator", "sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")

filtered_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("qty") > 145))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

metrics = (
    filtered_moves
    .select(
        "*",
        F.row_number().over(w_sku_id_by_unit_price).alias("rn"),
        F.ntile(5).over(w_sku_id_move_type_by_moved_at).alias("moved_at_quintile"),
        F.first("moved_at").over(w_operator_sku_id_by_unit_pric).alias("first_moved_at"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_move_type), F.lit(0)), 4).alias("unit_price_share_of_sku_id_move_type"),
    )
)

grouped = (
    metrics
    .filter(F.month(F.col("first_moved_at")).isin(1, 3))
    .groupBy("moved_at_quintile", "warehouse_id", "rn")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("move_type").isNotNull(), 1)).alias("n_move_type_is_not_null"),
    )
)

enriched = (
    grouped.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") > 11005), "left")
    .select("s.*", F.col("w.capacity").alias("warehouse_capacity"))
)

enriched_2 = (
    enriched.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.moved_at_quintile",
        "s.warehouse_id",
        "s.rn",
        "s.cnt",
        "s.n_move_type_is_not_null",
        "s.warehouse_capacity",
        F.col("w.capacity").alias("warehouse_capacity_2"),
    )
)

result = (
    enriched_2
    .filter(F.col("warehouse_capacity_2") >= 4912)
    .select(
        "moved_at_quintile",
        "warehouse_id",
        "rn",
        "cnt",
        "warehouse_capacity",
        "warehouse_capacity_2",
        (F.col("moved_at_quintile") - F.col("cnt")).alias("moved_at_quintile_minus_cnt"),
        F.abs(F.col("rn") - 5).alias("rn_distance"),
    )
    .orderBy(F.col("warehouse_id").asc(), F.col("warehouse_capacity_2").desc())
)
