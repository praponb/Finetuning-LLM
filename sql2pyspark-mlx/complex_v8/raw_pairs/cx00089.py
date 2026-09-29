from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.col("move_type") == "OUTBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 2 HOURS")).alias("moved_at_plus_2h"),
    )
)

annotated = valid_moves.withColumn("moved_at_year", F.year(F.col("moved_at")))

line_level = (
    annotated.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price").between(25.2, 436.9)), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.moved_at_plus_2h",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

without_matches = (
    line_level.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") > 93), "left_semi")
    .filter(F.col("move_operator") != "op_ann")
)

result = (
    without_matches
    .rollup("sku_id", "operator")
    .agg(
        F.grouping("sku_id").alias("is_total_sku_id"),
        F.round(F.sum("qty") / F.nullif(F.sum("move_unit_price"), F.lit(0)), 3).alias("qty_to_move_unit_price_ratio"),
    )
    .select(
        "sku_id",
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        "is_total_sku_id",
        "qty_to_move_unit_price_ratio",
    )
    .orderBy(F.col("sku_id").asc_nulls_last(), F.col("operator").asc_nulls_last())
)
