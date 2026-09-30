from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.col("operator") == "op_chan")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
        F.when(F.col("unit_price") >= 361.9, "large").when(F.col("unit_price") >= 125.9, "medium").otherwise("small").alias("unit_price_tier"),
    )
)

with_activity = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type").isin("OUTBOUND", "TRANSFER")) & (F.col("st.qty") <= 355), "left_semi")
)

without_matches = (
    with_activity.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.opened_on").between(F.lit("2024-02-19").cast("date"), F.lit("2025-07-03").cast("date"))), "left_semi")
)

result = (
    without_matches
    .select("id", "unit_price_bucket", "qty")
    .unpivot(
        ids=["id"],
        values=["unit_price_bucket", "qty"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "measure")
)
