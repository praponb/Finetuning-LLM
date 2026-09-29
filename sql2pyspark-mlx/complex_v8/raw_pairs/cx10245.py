from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
    .filter(F.col("operator").isin("op_bee", "op_ann"))
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

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_move_type_running))

sessions = (
    sessionized
    .groupBy("move_type", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

screened = (
    sessions
    .crossJoin(sessions.agg(F.avg("events").alias("overall_avg_events")))
    .filter(F.col("events") > F.col("overall_avg_events"))
    .drop("overall_avg_events")
)

filtered = (
    screened
    .crossJoin(screened.agg(F.max("events").alias("overall_max_events")))
    .filter(F.col("events") < F.col("overall_max_events"))
    .drop("overall_max_events")
)

result = (
    filtered
    .select(
        "move_type",
        "session_start",
        "events",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
    .orderBy(F.col("events").asc(), F.col("session_start").desc_nulls_first())
)
