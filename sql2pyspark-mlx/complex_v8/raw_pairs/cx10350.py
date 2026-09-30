from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(6, 10))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
    )
)

without_matches = (
    recent_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator").isin("op_ann", "op_dew")) & (F.col("st.unit_price") <= 277.6), "left_anti")
)

result = (
    without_matches
    .cube("operator", "sku_id", "warehouse_id")
    .agg(
        F.count(F.when(~F.col("move_type").isin("INBOUND", "TRANSFER"), 1)).alias("n_move_type_not_in_inbound_tra"),
    )
)
