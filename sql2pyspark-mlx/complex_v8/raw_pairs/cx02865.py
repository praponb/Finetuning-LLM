from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
    )
)

qualified = (
    scoped_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type").isin("OUTBOUND", "TRANSFER", "ADJUST")) & (F.col("st.qty") < 422), "left_anti")
    .filter(F.col("sku_id").isNotNull())
)

result = (
    qualified
    .groupBy("warehouse_id", "sku_id")
    .pivot("move_type", ["INBOUND", "OUTBOUND", "ADJUST", "TRANSFER"])
    .agg(F.avg("unit_price").alias("avg_v"), F.count("unit_price").alias("n"))
)
