from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stock_moves
    .filter(F.col("qty") > 189)
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

with_prev = base.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_operator_running))

per_session = (
    sessionized
    .groupBy("operator", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    per_session
    .select(
        "operator",
        "session_no",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        (F.col("session_start") + F.expr("INTERVAL 1 DAYS")).alias("session_start_plus_1d"),
        (F.col("session_start") + F.expr("INTERVAL 15 MINUTES")).alias("session_start_plus_15m"),
        F.greatest(F.col("duration_min"), F.col("session_unit_price")).alias("greatest_duration_min_session_unit_price"),
    )
)

enriched = (
    labeled
    .filter(F.col("session_end") >= F.lit("2025-01-09 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
        F.date_format(F.col("session_start"), "yyyy-MM-dd HH:00").alias("session_start_label"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

result = (
    enriched
    .select(
        "operator",
        "session_no",
        "greatest_duration_min_session_unit_price",
        "duration_min",
    )
    .unpivot(
        ids=["operator", "session_no"],
        values=["greatest_duration_min_session_unit_price", "duration_min"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
