from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = df_stock_moves

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_move_type_running))

session_summary = (
    sessionized
    .groupBy("move_type", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    session_summary
    .filter(F.col("event_count") < 5)
    .withColumn("move_type_clean", F.lower(F.trim(F.col("move_type"))))
)

result = (
    annotated
    .select(
        "move_type",
        "session_id",
        "event_count",
        "session_qty",
        "duration_min",
        F.date_trunc("quarter", F.col("session_end")).alias("session_end_quarter"),
    )
    .orderBy(F.col("session_qty").asc())
)
