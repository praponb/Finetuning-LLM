from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stock_moves
    .filter(F.col("qty") >= 189)
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

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_move_type_running))

sessions = (
    sessionized
    .groupBy("move_type", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    sessions
    .select(
        "*",
        F.date_format(F.col("session_start"), "yyyy-MM-dd HH:00").alias("session_start_label"),
        F.when(F.col("burst_id") >= 4, "HIGH").when(F.col("burst_id") >= 2, "MEDIUM").otherwise("LOW").alias("burst_id_tier"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

labeled = (
    annotated
    .filter(F.col("move_type") == "ADJUST")
    .select(
        "move_type",
        "burst_id",
        "session_end",
        "n_events",
        "session_qty",
        "burst_id_tier",
        F.concat(F.col("move_type"), F.lit("#"), F.col("session_qty").cast("string")).alias("move_type_ref"),
    )
)

result = (
    labeled
    .select(
        "move_type",
        "burst_id",
        "session_end",
        "n_events",
        "session_qty",
        "burst_id_tier",
        "move_type_ref",
    )
    .orderBy("move_type", F.col("move_type_ref").desc())
)
