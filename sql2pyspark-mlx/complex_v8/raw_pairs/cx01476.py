from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter((~F.col("operator").isin("op_eve", "op_dew")) & (F.to_date(F.col("moved_at")) < F.lit("2025-02-06").cast("date")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

filtered = (
    filtered_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.month(F.col("st.moved_at")).isin(1, 3)), "left_anti")
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-06").cast("date"))
)

result = (
    filtered
    .groupBy("sku_id")
    .pivot("move_type", ["INBOUND", "OUTBOUND", "ADJUST"])
    .agg(F.sum("qty"))
    .orderBy("sku_id")
)
