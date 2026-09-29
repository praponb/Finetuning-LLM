from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_events_operator_by_events = Window.partitionBy("events", "operator").orderBy("events", "operator", "session_id")
w_events_operator_by_events_ti = Window.partitionBy("events", "operator").orderBy("events")
w_events_operator_by_events_fu = Window.partitionBy("events", "operator").orderBy("events", "operator", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.abs(F.col("qty") - 300).alias("qty_distance"),
    )
)

with_prev = valid_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_operator_running))

session_summary = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    session_summary
    .select(
        "operator",
        "session_id",
        "events",
        "session_qty",
        F.lag("session_end", 3).over(w_events_operator_by_events).alias("prev3_session_end"),
        F.round(F.cume_dist().over(w_events_operator_by_events_ti), 3).alias("events_cume_dist"),
        F.first("duration_min").over(w_events_operator_by_events_fu).alias("first_duration_min"),
        F.last("session_end").over(w_events_operator_by_events_fu).alias("last_session_end"),
        F.ntile(10).over(w_events_operator_by_events).alias("events_decile"),
    )
)

result = (
    sequenced
    .filter(F.col("events_decile").between(1, 7))
    .select(
        "operator",
        "session_id",
        "events",
        "session_qty",
        "prev3_session_end",
        "events_cume_dist",
        "first_duration_min",
        "last_session_end",
        (F.col("events") + F.col("events_cume_dist")).alias("events_plus_events_cume_dist"),
    )
    .orderBy("first_duration_min", "operator", "session_id")
    .limit(5)
)
