from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("operator") == "op_ann")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.max("unit_price").alias("max_move_unit_price"),
    )
)

with_counts = (
    base.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.last_moved_at", "st.max_move_unit_price")
)

result = (
    with_counts
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "unit_price",
        "moved_at_label",
        "max_move_unit_price",
        F.abs(F.col("unit_price") - 323.9).alias("unit_price_distance"),
    )
    .orderBy(F.col("id").asc(), F.col("warehouse_id").desc())
)
