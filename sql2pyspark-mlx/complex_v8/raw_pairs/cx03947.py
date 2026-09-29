from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_start = Window.orderBy("session_start", "operator", "session_id")
w_session_id = Window.partitionBy("session_id")
w_session_end_to_date_by_sessi = Window.partitionBy(F.to_date("session_end")).orderBy(F.col("session_start").desc(), "operator", "session_id")

cleaned = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_eve", "op_ann"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_operator_running))

sessions = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

without_matches = (
    sessions
    .crossJoin(sessions.agg(F.max("session_qty").alias("overall_max_session_qty")))
    .filter(F.col("session_qty") < F.col("overall_max_session_qty"))
    .drop("overall_max_session_qty")
)

windowed = (
    without_matches
    .select(
        "operator",
        "session_id",
        "session_end",
        "events",
        "session_qty",
        "duration_min",
        F.lag("duration_min").over(w_all_by_session_start).alias("prev_duration_min"),
        F.size(F.collect_set("operator").over(w_session_id)).alias("distinct_operator_in_session_id"),
        (F.col("events") - F.lag("events").over(w_session_end_to_date_by_sessi)).alias("events_change"),
    )
)

result = (
    windowed
    .select(
        "operator",
        "session_end",
        "events",
        "session_qty",
        "duration_min",
        "prev_duration_min",
        "distinct_operator_in_session_id",
        "events_change",
        F.dayofweek(F.col("session_end")).alias("session_end_dayofweek"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
    .orderBy(F.col("operator").asc(), F.col("operator_slug").desc_nulls_first())
)
