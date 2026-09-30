from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_month_by_duratio = Window.partitionBy(F.month("session_end")).orderBy(F.col("duration_min").desc(), "warehouse_id", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_count_session_id = Window.partitionBy("event_count", "session_id")

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
        "unit_price",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_warehouse_id_running))

session_summary = (
    sessionized
    .groupBy("warehouse_id", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    session_summary
    .select(
        "warehouse_id",
        "session_id",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        F.first("event_count").over(w_session_end_month_by_duratio).alias("first_event_count"),
        F.size(F.collect_set("warehouse_id").over(w_event_count_session_id)).alias("distinct_warehouse_id_in_event_count_session_id"),
    )
)

result = (
    metrics
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "warehouse_id",
        "session_id",
        "session_start",
        "session_end",
        "duration_min",
        "distinct_warehouse_id_in_event_count_session_id",
        F.greatest(F.col("session_id"), F.col("event_count")).alias("greatest_session_id_event_count"),
    )
    .orderBy(F.col("warehouse_id").asc_nulls_last())
)
