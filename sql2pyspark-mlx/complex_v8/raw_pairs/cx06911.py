from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

with_prev = recent_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_warehouse_id_running))

sessions = (
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
    sessions
    .select(
        "warehouse_id",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        F.round(F.col("duration_min") * 1.07, 2).alias("duration_min_adj"),
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
        F.greatest(F.col("n_events"), F.col("duration_min")).alias("greatest_n_events_duration_min"),
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

result = (
    with_activity
    .filter(F.col("burst_id").between(2, 6))
    .select(
        "warehouse_id",
        "burst_id",
        "session_start",
        "session_end",
        "duration_min_adj",
        "duration_min_filled",
        "greatest_n_events_duration_min",
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 60, 1).alias("minutes_session_start_to_session_end"),
    )
    .orderBy(F.col("minutes_session_start_to_session_end").desc_nulls_first())
)
