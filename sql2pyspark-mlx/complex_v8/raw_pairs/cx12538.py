from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(~F.col("move_type").isin("TRANSFER", "OUTBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
    )
)

summary = (
    filtered
    .groupBy("sku_id", "operator", "move_type")
    .agg(
        F.round(100 * F.count(F.when(F.col("move_type").isin("OUTBOUND", "ADJUST"), 1)) / F.count("*"), 1).alias("pct_move_type_in_outbound_adjust"),
        F.count("*").alias("cnt"),
    )
)

enriched = (
    summary
    .filter(F.col("sku_id") >= 8)
    .select(
        "*",
        F.abs(F.col("cnt") - 6).alias("cnt_distance"),
        F.when(F.col("cnt") >= 10, "A").when(F.col("cnt") >= 5, "B").otherwise("C").alias("cnt_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

detail_rows_moves = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.sku_id",
        "s.operator",
        "s.move_type",
        "s.pct_move_type_in_outbound_adjust",
        "s.operator_slug",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

result = (
    detail_rows_moves
    .filter(F.col("move_id") <= 99)
    .select(
        "sku_id",
        "operator",
        "pct_move_type_in_outbound_adjust",
        "operator_slug",
        "move_move_type",
        F.when(F.col("move_move_type").isin("ADJUST", "INBOUND"), "group_a").when(F.col("move_move_type") == "TRANSFER", "group_b").otherwise("other").alias("move_move_type_group"),
    )
    .orderBy(F.col("move_move_type").desc(), "operator_slug")
)
