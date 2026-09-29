from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
    )
)

with_prev = recent.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_move_type_running))

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

without_matches = (
    per_session
    .crossJoin(per_session.agg(F.avg("duration_min").alias("overall_avg_duration_min")))
    .filter(F.col("duration_min") > F.col("overall_avg_duration_min"))
    .drop("overall_avg_duration_min")
)

result = (
    without_matches
    .select(
        "move_type",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "session_unit_price",
        "duration_min",
        F.date_format(F.col("session_start"), "yyyy-MM-dd").alias("session_start_label"),
    )
    .orderBy(F.col("burst_id").desc_nulls_first(), "move_type")
    .limit(20)
)
