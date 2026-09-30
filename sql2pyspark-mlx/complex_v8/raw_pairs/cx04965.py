from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

labeled = (
    valid
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "operator_move_type_label",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

without_matches = (
    labeled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
    .filter(F.col("sku_id").isNotNull())
)

result = (
    without_matches
    .filter(F.col("operator_move_type_label").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator_move_type_label",
        "move_type_len",
        "days_since_moved_at",
    )
    .orderBy(
        F.col("operator_move_type_label").desc(),
        F.col("days_since_moved_at").asc(),
    )
)
