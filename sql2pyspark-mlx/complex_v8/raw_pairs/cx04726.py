from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_year_session_no = Window.partitionBy(F.year("session_end"), "session_no")
w_operator_by_session_start_ti = Window.partitionBy("operator").orderBy(F.col("session_start").desc())

valid = (
    df_stock_moves
    .filter(F.col("operator").isin("op_dew", "op_chan"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_prev = valid.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_operator_running))

session_summary = (
    sessionized
    .groupBy("operator", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

with_windows = (
    session_summary
    .select(
        "*",
        F.size(F.collect_set("operator").over(w_session_end_year_session_no)).alias("distinct_operator_in_session_end_year_session_no"),
        F.count(F.lit(1)).over(w_session_end_year_session_no).alias("rows_in_session_end_year_session_no"),
        F.dense_rank().over(w_operator_by_session_start_ti).alias("session_start_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "operator",
        "session_no",
        "events",
        "distinct_operator_in_session_end_year_session_no",
        "rows_in_session_end_year_session_no",
        "session_start_dense_rank",
    )
    .orderBy(F.col("distinct_operator_in_session_end_year_session_no").asc())
)
