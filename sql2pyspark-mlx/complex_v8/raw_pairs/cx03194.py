from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("session_qty_tier").orderBy(F.col("duration_min").desc(), F.col("operator").desc(), F.col("session_no").desc())

filtered = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_operator_running))

session_summary = (
    sessionized
    .groupBy("operator", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    session_summary
    .select(
        "operator",
        "session_no",
        "session_end",
        "n_events",
        "session_qty",
        "duration_min",
        F.when(F.col("session_qty") >= 1300, "large").when(F.col("session_qty") >= 501, "medium").otherwise("small").alias("session_qty_tier"),
        F.round(F.col("session_no") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("session_no_per_duration_min"),
    )
)

latest_per_key = (
    derived
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "operator",
        "session_no",
        "session_end",
        "session_qty",
        "duration_min",
        "session_qty_tier",
        F.least(F.col("n_events"), F.col("duration_min")).alias("least_n_events_duration_min"),
    )
    .orderBy(
        F.col("least_n_events_duration_min").asc_nulls_last(),
        F.col("session_qty").asc_nulls_last(),
    )
)
