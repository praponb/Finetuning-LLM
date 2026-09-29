from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("qty").between(138, 300)) & (F.col("warehouse_id") >= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.abs(F.col("qty") - 216).alias("qty_distance"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

summarized_rows = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

tagged = (
    summarized_rows
    .filter(~F.col("move_type").isin("ADJUST", "INBOUND"))
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

result = (
    tagged
    .select(
        "id",
        F.col("qty_distance").alias("m_qty_distance"),
        F.col("move_count").alias("move_count_v"),
        F.col("operator_len").alias("m_operator_len"),
        F.col("days_since_moved_at").alias("m_days_since_moved_at"),
    )
    .unpivot(
        ids=["id"],
        values=["m_qty_distance", "move_count_v", "m_operator_len", "m_days_since_moved_at"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
