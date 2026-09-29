from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("qty") > 199) & (~F.col("move_type").isin("OUTBOUND", "INBOUND")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

move_rows = (
    filtered
    .filter((F.col("sku_id") >= 12) & (F.col("warehouse_id") <= 3))
    .select(
        F.col("warehouse_id").alias("entity_id"),
        F.col("moved_at").alias("event_time"),
    )
)

move_side = (
    df_stock_moves
    .filter((F.col("qty").between(67, 393)) & (F.col("moved_at") >= F.lit("2025-01-19 12:00:00").cast("timestamp")))
    .select(F.col("qty").alias("entity_id"), F.col("moved_at").alias("event_time"))
)

result = (
    move_rows
    .union(move_side)
    .distinct()
    .orderBy("entity_id", "event_time")
)
