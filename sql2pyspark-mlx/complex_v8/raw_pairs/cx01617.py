from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
    .filter(~F.col("move_type").isin("INBOUND", "ADJUST"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
    )
)

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_warehouse_id_running))

sessions = (
    sessionized
    .groupBy("warehouse_id", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("least_unit_price_qty").alias("session_least_unit_price_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    sessions
    .select(
        "warehouse_id",
        "session_id",
        "session_end",
        "n_events",
        "duration_min",
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 3600, 1).alias("hours_session_start_to_session_end"),
        F.date_format(F.col("session_end"), "MMM yyyy").alias("session_end_label"),
        F.abs(F.col("session_id") - 3).alias("session_id_distance"),
    )
)

result = (
    labeled
    .select(
        "warehouse_id",
        "session_end",
        "n_events",
        "duration_min",
        "hours_session_start_to_session_end",
        "session_end_label",
        "session_id_distance",
        F.length(F.trim(F.col("session_end_label"))).alias("session_end_label_len"),
        F.length(F.trim(F.col("session_end_label"))).alias("session_end_label_len_2"),
    )
    .orderBy(
        F.col("hours_session_start_to_session_end").asc(),
        F.col("n_events").asc(),
    )
)
