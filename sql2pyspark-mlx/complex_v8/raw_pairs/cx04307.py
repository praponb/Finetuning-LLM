from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter((F.col("operator").isNotNull()) & (F.col("warehouse_id") <= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.min("qty").alias("min_move_qty"),
    )
)

with_stats = (
    valid.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.last_moved_at", "st.min_move_qty")
)

result = (
    with_stats
    .groupBy("warehouse_id")
    .pivot("operator", ["op_ann", "op_bee", "op_eve"])
    .agg(F.count("qty_bucket"))
    .orderBy("warehouse_id")
)
