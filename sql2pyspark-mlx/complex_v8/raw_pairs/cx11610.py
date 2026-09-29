from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("warehouse_id") <= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") <= 298.9), "left_semi")
)

result = (
    screened
    .rollup("warehouse_id", "sku_id", "operator")
    .agg(F.round(F.avg("qty"), 2).alias("avg_qty"))
    .select(
        "warehouse_id",
        "sku_id",
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        "avg_qty",
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
    )
)
