from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("move_type") == "INBOUND") & (F.col("operator").isin("op_bee", "op_chan", "op_dew")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
    )
)

annotated = (
    filtered
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "operator_prefix",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

annotated_2 = (
    annotated
    .select(
        "*",
        F.month(F.col("moved_at_day")).alias("moved_at_day_month"),
        F.round(F.col("unit_price") * 1.1, 2).alias("unit_price_adj"),
        F.when(F.col("unit_price") >= 367.7, "HIGH").when(F.col("unit_price") >= 136.0, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
    )
)

tagged = (
    annotated_2
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "operator_prefix",
        "moved_at_day",
        "operator_clean",
        "unit_price_adj",
        "unit_price_tier",
        F.date_format(F.col("moved_at_day"), "yyyy-MM-dd").alias("moved_at_day_label"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("move_type").isin("ADJUST", "INBOUND"), 1)).alias("move_move_type_count"),
    )
)

with_stats = (
    tagged.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "sku_id",
        "operator_prefix",
        "moved_at_day",
        "operator_clean",
        "unit_price_tier",
        "moved_at_day_label",
        "qty_filled",
        F.substring(F.trim(F.col("unit_price_tier")), 1, 3).alias("unit_price_tier_prefix"),
        F.round(F.col("unit_price_adj") * 1.07, 2).alias("unit_price_adj_adj"),
    )
    .orderBy(F.col("moved_at_day_label").asc())
)
