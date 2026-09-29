from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_warehouse_id_running))

session_summary = (
    sessionized
    .groupBy("warehouse_id", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.min("qty").alias("min_move_qty"))
)

summarized_rows = (
    session_summary.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.min_move_qty")
)

enriched_warehouse = (
    summarized_rows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

result = (
    enriched_warehouse
    .filter(F.col("duration_min") > 84.0)
    .select(
        "warehouse_id",
        "session_no",
        "events",
        "min_move_qty",
        "warehouse_wh_code",
        F.year(F.col("session_start")).alias("session_start_year"),
    )
    .orderBy("min_move_qty", F.col("warehouse_id").desc())
)
