from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_stock_moves
    .filter(F.col("operator") == "op_ann")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

with_prev = prepared.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_warehouse_id_running))

sessions = (
    sessionized
    .groupBy("warehouse_id", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

combined_details_warehouse = (
    sessions.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.warehouse_id",
        "s.session_no",
        "s.session_start",
        "s.session_end",
        "s.session_qty",
        "s.duration_min",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

result = (
    combined_details_warehouse
    .select(
        "warehouse_id",
        "session_start",
        "session_end",
        "session_qty",
        "duration_min",
        "warehouse_city",
        "warehouse_capacity",
        F.coalesce(F.col("warehouse_city"), F.lit("UNKNOWN")).alias("warehouse_city_or_unknown"),
        (F.col("session_no") - F.col("duration_min")).alias("session_no_minus_duration_min"),
    )
    .orderBy("session_start", F.col("session_qty").asc())
)
