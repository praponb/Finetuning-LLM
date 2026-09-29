from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter((F.col("qty") <= 384) & (F.col("moved_at") < F.lit("2025-02-12 15:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
)

with_counts = (
    scoped_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.last_moved_at")
)

scored = (
    with_counts
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator",
        "unit_price",
        "last_moved_at",
        F.length(F.trim(F.col("move_type_or_unknown"))).alias("move_type_or_unknown_len"),
        F.round((F.unix_timestamp(F.col("last_moved_at")) - F.unix_timestamp(F.col("moved_at_year"))) / 3600, 1).alias("hours_moved_at_year_to_last_moved_at"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

profiled = (
    scored.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    profiled
    .groupBy("move_count", "last_moved_at")
    .pivot("move_type", ["OUTBOUND", "TRANSFER", "INBOUND", "ADJUST"])
    .agg(F.count("move_type_or_unknown_len"))
    .orderBy("move_count", "last_moved_at")
)
