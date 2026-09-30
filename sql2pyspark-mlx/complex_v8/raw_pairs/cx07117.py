from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_or_unknown = Window.partitionBy("move_type_or_unknown").orderBy("moved_at", "id")
w_move_type_or_unknown_running = Window.partitionBy("move_type_or_unknown").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
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
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

with_prev = scoped.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type_or_unknown))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_move_type_or_unknown_running))

session_summary = (
    sessionized
    .groupBy("move_type_or_unknown", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .filter(F.col("event_count") >= 2)
    .select(
        "move_type_or_unknown",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        F.lower(F.regexp_replace(F.trim(F.col("move_type_or_unknown")), " ", "_")).alias("move_type_or_unknown_slug"),
    )
)
