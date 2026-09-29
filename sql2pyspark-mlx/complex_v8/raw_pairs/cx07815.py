from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_move_type_by_ses = Window.partitionBy("event_count", "move_type").orderBy("session_start", "move_type", "session_no").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_count_move_type_by_ses_2 = Window.partitionBy("event_count", "move_type").orderBy("session_start", "move_type", "session_no").rowsBetween(-6, -1)
w_session_end_to_date_session = Window.partitionBy(F.to_date("session_end"), F.year("session_start"))

scoped_moves = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("quarter", F.col("moved_at")).alias("moved_at_quarter"),
    )
)

with_prev = scoped_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_move_type_running))

per_session = (
    sessionized
    .groupBy("move_type", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

with_windows = (
    per_session
    .select(
        "move_type",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        F.nth_value("event_count", 2).over(w_event_count_move_type_by_ses).alias("second_event_count"),
        F.max("event_count").over(w_event_count_move_type_by_ses_2).alias("event_count_max_6_prior"),
        F.sum("session_no").over(w_session_end_to_date_session).alias("session_end_to_date_session_start_year_sum_sessi"),
    )
)

derived = (
    with_windows
    .filter(F.month(F.col("session_end")).isin(2, 3))
    .select(
        "move_type",
        "session_no",
        "session_end",
        "event_count",
        "event_count_max_6_prior",
        "session_end_to_date_session_start_year_sum_sessi",
        F.concat(F.col("move_type"), F.lit("#"), F.col("event_count").cast("string")).alias("move_type_ref"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

result = (
    derived
    .select(
        "move_type",
        "session_no",
        "session_end",
        "event_count",
        "event_count_max_6_prior",
        "session_end_to_date_session_start_year_sum_sessi",
        "move_type_ref",
        "move_type_len",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_end")).alias("days_since_session_end"),
    )
    .orderBy("session_no", F.col("move_type").asc_nulls_last())
)
