from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) | (F.col("moved_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

exploded_detail_moves = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.qty").alias("move_qty"),
        "st.move_type",
    )
)

enriched = (
    exploded_detail_moves
    .filter(F.col("move_operator") == "op_dew")
    .select(
        "*",
        F.concat_ws("|", F.col("move_operator"), F.col("move_type")).alias("move_operator_move_type_label"),
        F.substring(F.trim(F.col("move_operator")), 1, 3).alias("move_operator_prefix"),
        F.concat(F.col("move_operator"), F.lit("#"), F.col("qty").cast("string")).alias("move_operator_ref"),
    )
)

result = (
    enriched
    .groupingSets(
        [["sku_id", "move_id"], ["sku_id"], ["move_id"], []],
        "sku_id", "move_id",
    )
    .agg(
        F.grouping("sku_id").alias("is_total_sku_id"),
        F.sum(F.when(F.col("move_type").isin("ADJUST", "INBOUND"), 1).otherwise(0)).alias("n_move_type_in_adjust_inbound"),
    )
)
