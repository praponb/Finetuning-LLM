from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_warehouse_id_running))

detail_rows = (
    sessionized.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") != "OUTBOUND"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.prev_moved_at",
        "s.new_session_flag",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

profiled = (
    detail_rows.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.avg_move_unit_price", "st.last_moved_at")
)

result = (
    profiled
    .select(
        "id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "prev_moved_at",
        "new_session_flag",
        "move_id",
        "move_move_type",
        "move_operator",
        "move_unit_price",
        "avg_move_unit_price",
    )
    .orderBy(F.col("id").asc_nulls_last(), "move_id")
    .limit(5)
)
