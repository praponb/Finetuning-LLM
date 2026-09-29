from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = df_stock_moves.filter(F.col("qty") >= 152)

with_prev = prepared.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_warehouse_id_running))

per_session = (
    sessionized
    .groupBy("warehouse_id", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    per_session
    .select(
        "warehouse_id",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "session_unit_price",
        "duration_min",
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket_2"),
        F.coalesce(F.col("event_count"), F.lit(0)).alias("event_count_filled"),
    )
)

joined = (
    enriched.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

result = (
    joined
    .select(
        "warehouse_id",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "session_unit_price",
        "duration_min",
        "duration_min_bucket_2",
        "event_count_filled",
        "warehouse_capacity",
        F.date_format(F.col("session_start"), "MMM yyyy").alias("session_start_label"),
    )
    .orderBy(
        F.col("session_start").desc_nulls_first(),
        F.col("duration_min").asc_nulls_last(),
    )
)
