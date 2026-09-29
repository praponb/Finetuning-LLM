from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(": ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

with_prev = recent_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_sku_id_running))

sessions = (
    sessionized
    .groupBy("sku_id", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .select(
        "sku_id",
        "session_end",
        "events",
        "duration_min",
        F.round(F.col("events") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("events_per_duration_min"),
    )
    .orderBy(F.col("session_end").desc(), F.col("events").desc())
)
