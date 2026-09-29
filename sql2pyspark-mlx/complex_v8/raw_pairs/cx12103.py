from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-02-12 10:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

qualified = (
    filtered_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty").between(99, 445)) & (F.col("st.unit_price") < 330.0), "left_semi")
)

operator_stats = (
    qualified
    .groupBy("operator")
    .agg(F.percentile_approx("qty", 0.5).alias("median_qty"))
)

outliers = (
    qualified.alias("s")
    .join(operator_stats.alias("g"), F.col("g.operator") == F.col("s.operator"), "inner")
    .filter(F.col("s.qty") >= F.col("g.median_qty"))
    .select(
        "s.*",
        "g.median_qty",
        F.round(F.col("s.qty") - F.col("g.median_qty"), 2).alias("qty_minus_median"),
    )
)

qualified_2 = (
    outliers.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
    .filter(F.col("unit_price") <= 363.6)
)

result = (
    qualified_2
    .groupBy("moved_at", "warehouse_id")
    .pivot("move_type", ["TRANSFER", "OUTBOUND", "ADJUST", "INBOUND"])
    .agg(F.sum("unit_price"))
)
