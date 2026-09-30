from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
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
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("move_count"),
        F.min("unit_price").alias("min_move_unit_price"),
    )
)

with_stats = (
    base_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.min_move_unit_price",
    )
)

result = (
    with_stats
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "operator",
        "last_moved_at",
        "move_count",
        "min_move_unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
    .orderBy(
        F.col("last_moved_at").asc(),
        F.col("min_move_unit_price").asc_nulls_last(),
    )
)
