from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("operator").isin("op_chan", "op_eve", "op_dew")) & (F.col("warehouse_id") <= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.abs(F.col("unit_price") - 161.4).alias("unit_price_distance"),
        F.when(F.col("qty") >= 415, "A").when(F.col("qty") >= 137, "B").otherwise("C").alias("qty_tier"),
        F.year(F.col("moved_at")).alias("moved_at_year"),
    )
)

combined_details_warehouse = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.unit_price_distance",
        "s.qty_tier",
        "s.moved_at_year",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

filtered = (
    combined_details_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") < 307.8), "left_semi")
)

without_matches = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

result = (
    without_matches
    .groupBy("sku_id")
    .pivot("move_type", ["ADJUST", "TRANSFER", "INBOUND", "OUTBOUND"])
    .agg(F.avg("qty").alias("avg_v"), F.max("qty").alias("max_v"))
)
