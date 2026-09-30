from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

enriched = (
    filtered.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.upper(F.trim(F.col("w.wh_code"))).like("C%"))
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

exploded_detail_moves = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.to_date(F.col("st.moved_at")) < F.lit("2025-02-12").cast("date")), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.unit_price",
        "s.warehouse_capacity",
        "s.warehouse_wh_code",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    exploded_detail_moves
    .filter(F.col("unit_price") < 330.0)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price",
        "warehouse_capacity",
        "move_operator",
        "move_moved_at",
    )
    .orderBy(F.col("move_moved_at").asc())
)
