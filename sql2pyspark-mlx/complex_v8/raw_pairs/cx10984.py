from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_row_count = Window.orderBy("row_count", "session_start_ym", "event_count")
w_all_by_row_count_ties = Window.orderBy("row_count")
w_row_count = Window.partitionBy("row_count")

filtered = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-03-02 12:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_warehouse_id_running))

session_summary = (
    sessionized
    .groupBy("warehouse_id", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

stats = (
    session_summary
    .groupBy(
        F.date_format("session_start", "yyyy-MM").alias("session_start_ym"),
        "event_count",
    )
    .agg(
        F.max("duration_min").alias("max_duration_min"),
        F.sum("event_count").alias("total_event_count"),
        F.count("*").alias("row_count"),
        F.percentile_approx("event_count", 0.5).alias("median_event_count"),
    )
    .filter(F.col("row_count") >= 2)
)

ranked = (
    stats
    .select(
        "session_start_ym",
        "event_count",
        "max_duration_min",
        "total_event_count",
        "row_count",
        F.round(F.percent_rank().over(w_all_by_row_count), 3).alias("row_count_percent_rank"),
        F.rank().over(w_all_by_row_count_ties).alias("row_count_rank"),
        F.count(F.lit(1)).over(w_row_count).alias("rows_in_row_count"),
        F.round(F.avg("max_duration_min").over(w_row_count), 2).alias("row_count_avg_max_duration_min"),
        F.max("row_count").over(w_row_count).alias("row_count_max_row_count"),
    )
)

result = (
    ranked
    .select(
        "session_start_ym",
        "event_count",
        "row_count_max_row_count",
        "rows_in_row_count",
        "row_count",
    )
    .unpivot(
        ids=["session_start_ym", "event_count"],
        values=["row_count_max_row_count", "rows_in_row_count", "row_count"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
