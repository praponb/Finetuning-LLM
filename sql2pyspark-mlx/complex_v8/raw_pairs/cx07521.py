from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_move_type_running))

per_session = (
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

with_activity = (
    per_session
    .crossJoin(per_session.agg(F.min("session_qty").alias("overall_min_session_qty")))
    .filter(F.col("session_qty") > F.col("overall_min_session_qty"))
    .drop("overall_min_session_qty")
)

tagged = with_activity.select("*", F.to_date(F.col("session_end")).alias("session_end_date"))

result = (
    tagged
    .groupBy("burst_id", "session_start")
    .pivot("move_type", ["ADJUST", "OUTBOUND", "INBOUND", "TRANSFER"])
    .agg(F.sum("duration_min"))
)
