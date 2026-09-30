from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_start_to_date_burst = Window.partitionBy(F.to_date("session_start"), "burst_id")
w_operator_burst_id_by_session = Window.partitionBy("operator", "burst_id").orderBy(F.col("session_start").desc(), "operator", "burst_id")
w_operator_by_session_end = Window.partitionBy("operator").orderBy("session_end", "operator", "burst_id")

prepared_moves = (
    df_stock_moves
    .filter(F.col("unit_price").between(60.4, 396.4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

with_prev = prepared_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_operator_running))

per_session = (
    sessionized
    .groupBy("operator", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("qty_filled").alias("session_qty_filled"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    per_session
    .select(
        "*",
        F.max("event_count").over(w_session_start_to_date_burst).alias("session_start_to_date_burst_id_max_event_count"),
        F.size(F.collect_set("operator").over(w_session_start_to_date_burst)).alias("distinct_operator_in_session_start_to_date_burst"),
        F.lead("session_end").over(w_operator_burst_id_by_session).alias("next_session_end"),
        F.row_number().over(w_operator_by_session_end).alias("rn"),
        F.round(F.avg("burst_id").over(w_session_start_to_date_burst), 2).alias("session_start_to_date_burst_id_avg_burst_id"),
    )
)

tagged = (
    windowed
    .select(
        "operator",
        "burst_id",
        "session_start",
        "session_qty_filled",
        "session_start_to_date_burst_id_max_event_count",
        "distinct_operator_in_session_start_to_date_burst",
        "next_session_end",
        "rn",
        "session_start_to_date_burst_id_avg_burst_id",
        F.abs(F.col("rn") - 4).alias("rn_distance"),
        F.concat(F.col("operator"), F.lit("#"), F.col("session_qty_filled").cast("string")).alias("operator_ref"),
    )
)

result = (
    tagged
    .select(
        "operator",
        "burst_id",
        "session_start",
        "session_qty_filled",
        "distinct_operator_in_session_start_to_date_burst",
        "next_session_end",
        "session_start_to_date_burst_id_avg_burst_id",
        "rn_distance",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("session_start")).alias("days_since_session_start"),
    )
    .orderBy(
        F.col("session_start_to_date_burst_id_avg_burst_id").desc(),
        F.col("session_start").asc(),
    )
)
