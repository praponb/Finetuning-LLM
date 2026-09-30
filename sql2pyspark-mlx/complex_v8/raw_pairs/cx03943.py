from pyspark.sql import functions as F

valid_moves = (
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

line_level_moves = (
    valid_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    line_level_moves
    .cube("move_id", "move_type")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("move_type").isNotNull(), 1)).alias("n_move_type_is_not_null"),
        F.round(F.stddev("move_unit_price"), 2).alias("stddev_move_unit_price"),
    )
    .select(
        "move_id",
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "cnt",
        "n_move_type_is_not_null",
        "stddev_move_unit_price",
    )
)
