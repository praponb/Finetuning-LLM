from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_moves = (
    df_stock_moves
    .filter(F.col("unit_price") > 164.1)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
    )
)

with_prev = scoped_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_operator_running))

per_session = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

screened = (
    per_session
    .crossJoin(per_session.agg(F.avg("session_id").alias("overall_avg_session_id")))
    .filter(F.col("session_id") > F.col("overall_avg_session_id"))
    .drop("overall_avg_session_id")
)

labeled = (
    screened
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 60, 1).alias("minutes_session_start_to_session_end"),
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 60, 1).alias("minutes_session_start_to_session_end_2"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
    )
)

result = (
    labeled
    .select(
        "operator",
        "session_id",
        "session_start",
        "session_end",
        "session_unit_price",
        "duration_min",
        "minutes_session_start_to_session_end",
        "minutes_session_start_to_session_end_2",
        "operator_prefix",
        F.round(F.col("session_unit_price") * 0.25, 2).alias("session_unit_price_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
    .orderBy(
        "session_start",
        F.col("minutes_session_start_to_session_end").asc_nulls_last(),
    )
)
