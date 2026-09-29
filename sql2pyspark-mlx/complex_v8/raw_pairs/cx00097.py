from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter((F.col("move_type") == "ADJUST") & (F.col("warehouse_id") <= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

joined_sku = (
    recent_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

line_level = (
    joined_sku.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.sku_unit_cost",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

labeled = (
    line_level
    .select(
        "*",
        F.length(F.trim(F.col("move_move_type"))).alias("move_move_type_len"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

result = (
    labeled
    .select(
        "id",
        "sku_id",
        "operator",
        "sku_unit_cost",
        "move_qty",
        "move_move_type",
        "move_moved_at",
        "move_move_type_len",
        "operator_or_unknown",
        F.abs(F.col("sku_unit_cost") - 251.2).alias("sku_unit_cost_distance"),
        (F.floor(F.col("move_move_type_len") / 2) * 2).alias("move_move_type_len_bucket"),
    )
    .orderBy(
        F.col("sku_unit_cost_distance").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
