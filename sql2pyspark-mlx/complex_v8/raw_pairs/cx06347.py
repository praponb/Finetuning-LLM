from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = df_stock_moves.filter(F.col("operator") == "op_ann")

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_warehouse_id_running))

per_session = (
    sessionized
    .groupBy("warehouse_id", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    per_session
    .filter(F.col("session_end") >= F.lit("2025-01-21 09:00:00").cast("timestamp"))
    .withColumn("n_events_filled", F.coalesce(F.col("n_events"), F.lit(0)))
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

with_counts = (
    enriched.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    with_counts
    .select(
        "warehouse_id",
        "session_start",
        "n_events",
        "move_count",
        F.when(F.col("n_events_filled") >= 4, "HIGH").when(F.col("n_events_filled") >= 2, "MEDIUM").otherwise("LOW").alias("n_events_filled_tier"),
        F.date_trunc("year", F.col("session_end")).alias("session_end_year"),
    )
    .orderBy("n_events_filled_tier")
)
