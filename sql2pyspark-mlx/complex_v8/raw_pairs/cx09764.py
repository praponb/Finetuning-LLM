from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_start_ties = Window.orderBy(F.col("session_start").desc())
w_move_type_session_end_year = Window.partitionBy("move_type", F.year("session_end"))
w_events_by_events = Window.partitionBy("events").orderBy(F.col("events").desc_nulls_first(), "move_type", "burst_id")

cleaned = (
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

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_move_type_running))

per_session = (
    sessionized
    .groupBy("move_type", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    per_session
    .select(
        "move_type",
        "burst_id",
        "session_start",
        "session_unit_price",
        "duration_min",
        F.rank().over(w_all_by_session_start_ties).alias("session_start_rank"),
        F.min("burst_id").over(w_move_type_session_end_year).alias("move_type_session_end_year_min_burst_id"),
        F.row_number().over(w_events_by_events).alias("events_seq"),
        F.round(F.col("duration_min") - F.avg("duration_min").over(w_move_type_session_end_year), 2).alias("duration_min_vs_move_type_session_end_year_avg"),
    )
)

result = (
    sequenced
    .select(
        "move_type",
        "burst_id",
        "session_start",
        "session_unit_price",
        "duration_min",
        "session_start_rank",
        "move_type_session_end_year_min_burst_id",
        "events_seq",
        "duration_min_vs_move_type_session_end_year_avg",
    )
    .orderBy(
        "duration_min",
        "session_start",
        "move_type",
        "burst_id",
    )
    .limit(5)
)
