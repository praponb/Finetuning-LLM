from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

screened = (
    scoped_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type").isin("ADJUST", "OUTBOUND", "TRANSFER")) & (F.col("st.qty") <= 415), "left_semi")
    .filter(F.col("qty_minus_unit_price") >= 300.7)
)

with_parent_sku = (
    screened.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator_len",
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

without_matches = (
    with_parent_sku.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
)

result = (
    without_matches
    .filter(~F.col("sku_is_active"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "sku_is_active",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
        F.when(F.col("move_type").isin("TRANSFER", "OUTBOUND"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)
