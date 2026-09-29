from pyspark.sql import functions as F

valid_warehouses = (
    df_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.max("moved_at").alias("last_moved_at_2"),
    )
)

with_stats = (
    valid_warehouses.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.last_moved_at", "st.last_moved_at_2")
)

tagged = (
    with_stats
    .filter(F.col("last_moved_at") < F.lit("2025-02-17 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.date_trunc("week", F.col("last_moved_at")).alias("last_moved_at_week"),
    )
)

filtered = (
    tagged.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left_semi")
)

result = (
    filtered
    .groupBy("last_moved_at", "last_moved_at_2")
    .pivot("city", ["Singapore", "Bangkok"])
    .agg(F.sum("capacity"))
    .orderBy("last_moved_at", "last_moved_at_2")
)
