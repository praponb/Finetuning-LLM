from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.col("unit_price") > 95.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

qualified = (
    screened.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "OUTBOUND"), "left_anti")
    .filter(F.col("moved_at") >= F.lit("2025-02-12 10:00:00").cast("timestamp"))
)

result = (
    qualified
    .select(
        "id",
        F.col("qty").alias("m_qty"),
        F.col("qty_bucket").alias("qty_bucket_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_qty", "qty_bucket_v"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
