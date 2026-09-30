from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
    )
)

with_prev = recent_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_move_type_running))

session_summary = (
    sessionized
    .groupBy("move_type", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

per_group = (
    session_summary
    .groupBy("events")
    .agg(
        F.max("session_end").alias("last_session_end"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.min("session_start").alias("first_session_start"),
    )
)

tagged = (
    per_group
    .select(
        "events",
        "last_session_end",
        "first_session_start",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_session_end")).alias("days_since_last_session_end"),
        F.abs(F.col("distinct_move_types") - 6).alias("distinct_move_types_distance"),
        F.round((F.unix_timestamp(F.col("first_session_start")) - F.unix_timestamp(F.col("last_session_end"))) / 3600, 1).alias("hours_last_session_end_to_first_session_start"),
    )
)

result = (
    tagged
    .select(
        "events",
        "last_session_end",
        "first_session_start",
        "days_since_last_session_end",
        "distinct_move_types_distance",
        "hours_last_session_end_to_first_session_start",
        (F.col("first_session_start") + F.expr("INTERVAL 15 MINUTES")).alias("first_session_start_plus_15m"),
    )
    .orderBy("last_session_end")
)
