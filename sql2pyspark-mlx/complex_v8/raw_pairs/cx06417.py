from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.month(F.col("moved_at")).isin(1, 2)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(": ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

qualified = (
    valid.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isNotNull()), "left_semi")
    .filter(F.col("sku_id") <= 13)
)

result = (
    qualified
    .cube("sku_id", "move_type", "operator")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.count("*").alias("row_count"),
    )
    .select(
        "sku_id",
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        "total_unit_price",
        "row_count",
    )
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
    )
)
