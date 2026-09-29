from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_moves = (
    df_stock_moves
    .filter(F.col("sku_id") <= 13)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

with_prev = valid_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_operator_running))

session_summary = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

per_group = (
    session_summary
    .filter(F.col("session_qty") < 1800)
    .groupBy("operator")
    .agg(
        F.percentile_approx("n_events", 0.5).alias("median_n_events"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("operator") == "op_chan", F.col("session_id")).otherwise(0)).alias("session_id_operator_op_chan"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.count(F.when(F.col("operator").isin("op_ann", "op_bee", "op_dew"), 1)).alias("n_operator_in_op_ann_op_bee_op"),
        F.max("session_id").alias("max_session_id_check"),
    )
    .filter(F.col("max_session_id_check") > 2)
    .drop("max_session_id_check")
)

result = (
    per_group
    .select(
        "operator",
        "median_n_events",
        "n_rows",
        "session_id_operator_op_chan",
        "distinct_operators",
        "n_operator_in_op_ann_op_bee_op",
        (F.col("distinct_operators") - F.col("median_n_events")).alias("distinct_operators_minus_median_n_events"),
    )
    .orderBy(
        F.col("n_operator_in_op_ann_op_bee_op").asc_nulls_last(),
        F.col("distinct_operators_minus_median_n_events").asc(),
        "operator",
    )
    .limit(10)
)
