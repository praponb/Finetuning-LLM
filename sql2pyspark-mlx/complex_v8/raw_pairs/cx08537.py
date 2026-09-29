from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id = Window.partitionBy("session_id")
w_warehouse_id_by_session_qty = Window.partitionBy("warehouse_id").orderBy("session_qty", "warehouse_id", "session_id")

base_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("id").cast("string")).alias("operator_ref"),
    )
)

with_prev = base_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_warehouse_id_running))

per_session = (
    sessionized
    .groupBy("warehouse_id", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    per_session
    .select(
        "*",
        F.size(F.collect_set("warehouse_id").over(w_session_id)).alias("distinct_warehouse_id_in_session_id"),
        F.lag("duration_min", 3).over(w_warehouse_id_by_session_qty).alias("prev3_duration_min"),
        F.min("duration_min").over(w_session_id).alias("session_id_min_duration_min"),
        F.lead("session_start").over(w_warehouse_id_by_session_qty).alias("next_session_start"),
    )
)

filtered = (
    ranked.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") >= 148), "left_anti")
)

result = (
    filtered
    .select(
        "warehouse_id",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        "session_qty",
        "duration_min",
        "distinct_warehouse_id_in_session_id",
        "session_id_min_duration_min",
        F.round(F.col("prev3_duration_min") * 0.25, 2).alias("prev3_duration_min_adj"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("session_start")).alias("days_since_session_start"),
    )
    .orderBy("session_end", F.col("n_events").asc())
)
