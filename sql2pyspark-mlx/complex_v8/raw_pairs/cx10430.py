from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id_n_events = Window.partitionBy("session_id", "n_events")

recent = (
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
        F.concat(F.col("operator"), F.lit("#"), F.col("sku_id").cast("string")).alias("operator_ref"),
    )
)

with_prev = recent.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_move_type_running))

per_session = (
    sessionized
    .groupBy("move_type", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

with_windows = (
    per_session
    .withColumn("distinct_move_type_in_session_id_n_events", F.size(F.collect_set("move_type").over(w_session_id_n_events)))
    .withColumn("rows_in_session_id_n_events", F.count(F.lit(1)).over(w_session_id_n_events))
)

grouped = (
    with_windows
    .groupBy(F.to_date("session_start").alias("session_start_day"))
    .agg(
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.min("session_id").alias("min_session_id"),
    )
)

result = (
    grouped
    .filter(F.col("session_start_day").isNotNull())
    .select("session_start_day", "move_type_list")
    .orderBy(F.col("session_start_day").desc())
    .limit(10)
)
