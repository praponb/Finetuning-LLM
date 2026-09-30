from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.col("unit_price") < 313.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("move_type") == "INBOUND", 1)).alias("move_move_type_count"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

result = (
    summarized_rows
    .groupBy("sku_id", "warehouse_id")
    .pivot("operator", ["op_chan", "op_dew"])
    .agg(F.avg("qty"))
    .orderBy("sku_id", "warehouse_id")
)
