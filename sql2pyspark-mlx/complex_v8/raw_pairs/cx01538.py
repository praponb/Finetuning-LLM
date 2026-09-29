from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_by_duration_min_ful = Window.partitionBy("burst_id").orderBy("duration_min", "move_type", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_n_events = Window.orderBy("n_events", "move_type", "burst_id")
w_n_events = Window.partitionBy("n_events")

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_move_type_running))

per_session = (
    sessionized
    .groupBy("move_type", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    per_session
    .select(
        "*",
        F.nth_value("duration_min", 2).over(w_burst_id_by_duration_min_ful).alias("second_duration_min"),
        F.rank().over(w_all_by_n_events).alias("n_events_rank"),
        F.round(F.col("burst_id") / F.nullif(F.sum("burst_id").over(w_n_events), F.lit(0)), 4).alias("burst_id_share_of_n_events"),
        F.round(F.avg("burst_id").over(w_n_events), 2).alias("n_events_avg_burst_id"),
        F.last("session_start").over(w_burst_id_by_duration_min_ful).alias("last_session_start"),
    )
)

result = (
    sequenced
    .select(
        "move_type",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        "second_duration_min",
        "burst_id_share_of_n_events",
        "n_events_avg_burst_id",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.date_format(F.col("session_end"), "yyyy-MM").alias("session_end_label"),
    )
    .orderBy(F.col("move_type_slug").desc())
)
