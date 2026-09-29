from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_stock_moves
    .filter(F.col("sku_id") >= 8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

with_prev = prepared.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_sku_id_running))

sessions = (
    sessionized
    .groupBy("sku_id", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .select(
        "sku_id",
        "session_id",
        "session_start",
        "duration_min",
        (F.col("duration_min") + F.col("session_id")).alias("duration_min_plus_session_id"),
        F.round(F.col("n_events") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("n_events_per_duration_min"),
    )
    .orderBy(F.col("sku_id").asc(), "session_id")
    .limit(10)
)
