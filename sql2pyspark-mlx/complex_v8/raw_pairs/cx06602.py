from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
    )
)

with_prev = scoped.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_operator_running))

per_session = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    per_session
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 3600, 1).alias("hours_session_start_to_session_end"),
    )
)

stats = (
    labeled
    .groupBy("event_count", "operator", "session_id")
    .agg(
        F.round(F.stddev("hours_session_start_to_session_end"), 2).alias("stddev_hours_session_start_to_session_end"),
        F.round(F.sum("session_id") / F.nullif(F.sum("event_count"), F.lit(0)), 3).alias("session_id_to_event_count_ratio"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.min("session_end").alias("first_session_end"),
        F.sum("event_count").alias("total_event_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    stats
    .filter(F.size(F.col("operator_list")) >= 1)
    .select(
        "event_count",
        "operator",
        "stddev_hours_session_start_to_session_end",
        "session_id_to_event_count_ratio",
        "operator_list",
        "first_session_end",
        "total_event_count",
        "cnt",
    )
    .orderBy(F.col("total_event_count").asc(), F.col("cnt").desc_nulls_first())
)
