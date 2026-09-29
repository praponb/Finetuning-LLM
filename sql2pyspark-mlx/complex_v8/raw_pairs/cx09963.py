from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(" / ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.abs(F.col("unit_price") - 160.8).alias("unit_price_distance"),
        F.when(F.col("unit_price") >= 293.2, "large").when(F.col("unit_price") >= 236.5, "medium").otherwise("small").alias("unit_price_tier"),
    )
)

detail_rows = (
    filtered_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.unit_price_distance",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

summarized_rows = (
    detail_rows.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    summarized_rows
    .select(
        "id",
        "move_id",
        F.col("move_unit_price").alias("move_unit_price_v"),
        F.col("unit_price_distance").alias("unit_price_distance_v"),
        F.col("unit_price").alias("m_unit_price"),
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["move_unit_price_v", "unit_price_distance_v", "m_unit_price"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
