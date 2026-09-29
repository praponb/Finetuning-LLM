from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("move_type").isin("ADJUST", "OUTBOUND", "INBOUND")) & (F.month(F.col("moved_at")).isin(1, 2)))
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

with_details_warehouse = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity").between(2614, 11506)), "inner")
    .filter(F.length(F.trim(F.col("w.wh_code"))) >= 4)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
)

profiled = (
    with_details_warehouse.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.last_moved_at")
)

result = (
    profiled
    .filter(F.col("sku_id") >= 16)
    .select(
        "id",
        "operator",
        "warehouse_wh_code",
        "last_moved_at",
        F.round((F.unix_timestamp(F.col("moved_at")) - F.unix_timestamp(F.col("last_moved_at"))) / 3600, 1).alias("hours_last_moved_at_to_moved_at"),
    )
    .orderBy("last_moved_at", "hours_last_moved_at_to_moved_at", "id")
    .limit(10)
)
