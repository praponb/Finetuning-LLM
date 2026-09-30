from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter(F.col("unit_price") > 61.2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

detail_rows = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id") <= 7), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

result = (
    detail_rows
    .groupBy("move_sku_id")
    .pivot("move_type", ["INBOUND", "TRANSFER", "ADJUST", "OUTBOUND"])
    .agg(F.avg("qty"))
    .orderBy("move_sku_id")
)
