from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("unit_price") >= 111.7))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.concat_ws("-", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
        F.concat_ws("|", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

scored = (
    filtered
    .filter(F.col("operator_move_type_label").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "operator_move_type_label",
        F.concat(F.col("operator_move_type_label"), F.lit("#"), F.col("id").cast("string")).alias("operator_move_type_label_ref"),
        F.when(F.col("unit_price") >= 361.9, "HIGH").when(F.col("unit_price") >= 164.1, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
        F.length(F.trim(F.col("move_type_operator_label"))).alias("move_type_operator_label_len"),
    )
)

with_activity = (
    scored.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") <= 349), "left_semi")
)

without_matches = (
    with_activity.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

annotated = (
    without_matches
    .filter(F.col("warehouse_id") <= 2)
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator_move_type_label",
        "operator_move_type_label_ref",
        "unit_price_tier",
        "move_type_operator_label_len",
        F.concat(F.col("unit_price_tier"), F.lit("#"), F.col("sku_id").cast("string")).alias("unit_price_tier_ref"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.round(F.avg("qty"), 2).alias("avg_move_qty"),
    )
)

summarized_rows = (
    annotated.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.avg_move_qty",
    )
)

result = (
    summarized_rows
    .select("id", "move_count", "move_type_operator_label_len")
    .unpivot(
        ids=["id"],
        values=["move_count", "move_type_operator_label_len"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
