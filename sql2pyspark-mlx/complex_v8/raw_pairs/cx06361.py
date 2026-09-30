from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_duration_min_tier_cnt_by_cnt = Window.partitionBy("duration_min_tier", "cnt").orderBy("cnt", "duration_min_tier")
w_duration_min_tier = Window.partitionBy("duration_min_tier")

filtered = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
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

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_warehouse_id_running))

per_session = (
    sessionized
    .groupBy("warehouse_id", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

summary = (
    per_session
    .groupBy(
        F.when(F.col("duration_min") >= 204.0, "large").when(F.col("duration_min") >= 60.0, "medium").otherwise("small").alias("duration_min_tier"),
    )
    .agg(
        F.round(F.sum("session_unit_price"), 2).alias("total_session_unit_price"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        F.round(F.sum("n_events") / F.nullif(F.sum("burst_id"), F.lit(0)), 3).alias("n_events_to_burst_id_ratio"),
        F.count("*").alias("cnt"),
        F.round(F.sum("session_unit_price") / F.nullif(F.sum("burst_id"), F.lit(0)), 3).alias("session_unit_price_to_burst_id_ratio"),
    )
)

metrics = (
    summary
    .select(
        "*",
        F.row_number().over(w_duration_min_tier_cnt_by_cnt).alias("seq_no"),
        F.count(F.lit(1)).over(w_duration_min_tier).alias("rows_in_duration_min_tier"),
        F.round(F.col("n_events_to_burst_id_ratio") / F.nullif(F.sum("n_events_to_burst_id_ratio").over(w_duration_min_tier), F.lit(0)), 4).alias("n_events_to_burst_id_ratio_share_of_duration_min"),
        F.round(F.col("avg_duration_min") - F.avg("avg_duration_min").over(w_duration_min_tier), 2).alias("avg_duration_min_vs_duration_min_tier_avg"),
        F.sum("cnt").over(w_duration_min_tier).alias("duration_min_tier_sum_cnt"),
    )
)

result = (
    metrics
    .select(
        "duration_min_tier",
        "n_events_to_burst_id_ratio",
        "cnt",
        "rows_in_duration_min_tier",
        "avg_duration_min_vs_duration_min_tier_avg",
        "duration_min_tier_sum_cnt",
    )
    .orderBy(
        F.col("rows_in_duration_min_tier").desc_nulls_first(),
        "avg_duration_min_vs_duration_min_tier_avg",
    )
)
