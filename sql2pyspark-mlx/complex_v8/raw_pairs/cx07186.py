from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_moves = (
    df_stock_moves
    .filter(F.col("unit_price") <= 398.5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_prev = filtered_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_move_type_running))

per_session = (
    sessionized
    .groupBy("move_type", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    per_session
    .select(
        "*",
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("session_end")).alias("days_since_session_end"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.length(F.trim(F.col("move_type_clean"))).alias("move_type_clean_len"),
        F.when(F.col("session_no") >= 5, "HIGH").when(F.col("session_no") >= 3, "MEDIUM").otherwise("LOW").alias("session_no_tier"),
        (F.floor(F.col("days_since_session_end") / 100) * 100).alias("days_since_session_end_bucket"),
    )
)

result = (
    scored
    .select(
        "move_type",
        "session_no",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        "move_type_clean",
        "days_since_session_end",
        "move_type_clean_len",
        "session_no_tier",
        "days_since_session_end_bucket",
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 3600, 1).alias("hours_session_end_to_session_start"),
    )
    .orderBy("session_no_tier", "events")
)
