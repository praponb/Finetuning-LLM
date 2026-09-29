from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id_by_qty = Window.partitionBy("sku_id", "warehouse_id").orderBy("qty", "id")
w_warehouse_id_moved_at_year = Window.partitionBy("warehouse_id", F.year("moved_at"))

base_moves = (
    df_stock_moves
    .filter(F.col("operator").isin("op_bee", "op_ann"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
        F.abs(F.col("qty") - 154).alias("qty_distance"),
    )
)

without_matches = (
    base_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (~F.col("st.move_type").isin("ADJUST", "TRANSFER")) & (F.col("st.moved_at") >= F.lit("2025-01-31 15:00:00").cast("timestamp")), "left_semi")
)

ranked = (
    without_matches
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "unit_price",
        "qty_distance",
        F.round(F.cume_dist().over(w_sku_id_warehouse_id_by_qty), 3).alias("qty_cume_dist"),
        F.dense_rank().over(w_sku_id_warehouse_id_by_qty).alias("qty_dense_rank"),
        F.count(F.lit(1)).over(w_warehouse_id_moved_at_year).alias("rows_in_warehouse_id_moved_at_year"),
        F.size(F.collect_set("operator").over(w_warehouse_id_moved_at_year)).alias("distinct_operator_in_warehouse_id_moved_at_year"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type") == "ADJUST", 1)).alias("move_move_type_count"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

with_counts = (
    ranked.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        "st.last_moved_at",
    )
)

totals = (
    with_counts
    .filter(F.col("qty").between(138, 387))
    .groupBy(
        F.to_date("moved_at").alias("moved_at_day"),
        "warehouse_id",
        "distinct_operator_in_warehouse_id_moved_at_year",
    )
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("cnt"),
        F.round(F.sum("move_move_type_count") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("move_move_type_count_to_qty_ratio"),
        F.round(F.sum("rows_in_warehouse_id_moved_at_year") / F.nullif(F.sum("unit_price"), F.lit(0)), 3).alias("rows_in_warehouse_id_moved_at_year_to_unit_price"),
        F.round(F.stddev("qty_distance"), 2).alias("stddev_qty_distance"),
        F.max("moved_at").alias("last_moved_at_2"),
    )
)

qualified = (
    totals.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id") <= 7) & (F.col("st.unit_price").between(161.4, 328.3)), "left_anti")
)

with_details_warehouse = (
    qualified.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    with_details_warehouse
    .select(
        "moved_at_day",
        "warehouse_id",
        "move_move_type_count_to_qty_ratio",
        "stddev_qty_distance",
        "last_moved_at_2",
        "warehouse_opened_on",
        "warehouse_city",
        F.substring(F.trim(F.col("warehouse_city")), 1, 4).alias("warehouse_city_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_moved_at")).alias("days_since_first_moved_at"),
    )
    .orderBy(F.col("warehouse_city_prefix").desc())
)
