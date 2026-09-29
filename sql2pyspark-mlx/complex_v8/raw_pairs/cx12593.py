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
        "qty",
        "moved_at",
        "unit_price",
    )
)

with_prev = recent.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_move_type_running))

session_summary = (
    sessionized
    .groupBy("move_type", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

summary = (
    session_summary
    .filter(F.col("move_type") == "INBOUND")
    .groupBy("burst_id", "events", "move_type")
    .agg(
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.min("session_start").alias("first_session_start"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("burst_id"), 2).alias("stddev_burst_id"),
        F.count(F.when(F.col("move_type").isin("OUTBOUND", "INBOUND", "ADJUST"), 1)).alias("n_move_type_in_outbound_inboun"),
    )
)

result = (
    summary
    .select(
        "burst_id",
        "move_type_list",
        "first_session_start",
        "stddev_burst_id",
        "n_move_type_in_outbound_inboun",
    )
    .orderBy("first_session_start")
)
