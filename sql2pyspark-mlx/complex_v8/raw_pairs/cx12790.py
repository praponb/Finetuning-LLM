from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id_by_session_end_ti = Window.partitionBy("session_id").orderBy("session_end")
w_session_end_year_session_id = Window.partitionBy(F.year("session_end"), "session_id")
w_session_id_by_session_end = Window.partitionBy("session_id").orderBy("session_end", "operator", "session_id")

valid_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

with_prev = valid_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_operator_running))

per_session = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered = (
    per_session
    .crossJoin(per_session.agg(F.min("duration_min").alias("overall_min_duration_min")))
    .filter(F.col("duration_min") > F.col("overall_min_duration_min"))
    .drop("overall_min_duration_min")
)

windowed = (
    filtered
    .select(
        "*",
        F.rank().over(w_session_id_by_session_end_ti).alias("session_end_rank"),
        F.size(F.collect_set("operator").over(w_session_end_year_session_id)).alias("distinct_operator_in_session_end_year_session_id"),
        F.sum("n_events").over(w_session_end_year_session_id).alias("session_end_year_session_id_sum_n_events"),
        F.round(F.percent_rank().over(w_session_id_by_session_end), 3).alias("session_end_percent_rank"),
    )
)

result = (
    windowed
    .filter(F.col("session_end_year_session_id_sum_n_events") < 54)
    .select(
        "operator",
        "session_id",
        "session_start",
        "n_events",
        "session_end_rank",
        "distinct_operator_in_session_end_year_session_id",
        "session_end_year_session_id_sum_n_events",
        "session_end_percent_rank",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.when(F.col("operator").isin("op_dew", "op_chan"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
    )
    .orderBy(F.col("session_id").asc_nulls_last())
)
