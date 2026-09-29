from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_moves = (
    df_stock_moves
    .filter(F.col("unit_price") >= 112.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

with_prev = cleaned_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

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
        F.count("*").alias("event_count"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

stats = (
    session_summary
    .filter(F.col("session_no") < 4)
    .groupBy("move_type")
    .agg(
        F.round(F.sum("session_qty") / F.nullif(F.sum("session_no"), F.lit(0)), 3).alias("session_qty_to_session_no_ratio"),
        F.count("*").alias("cnt"),
        F.round(F.sum("duration_min") / F.nullif(F.sum("session_no"), F.lit(0)), 3).alias("duration_min_to_session_no_ratio"),
        F.max("event_count").alias("max_event_count_check"),
    )
    .filter(F.col("max_event_count_check") > 3)
    .drop("max_event_count_check")
)

result = (
    stats
    .filter(F.col("duration_min_to_session_no_ratio").between(2.0, 8.0))
    .select(
        "move_type",
        "session_qty_to_session_no_ratio",
        "cnt",
        "duration_min_to_session_no_ratio",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
    .orderBy(F.col("move_type_slug").asc())
)
