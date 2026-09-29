from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_events_distinct_operators = Window.partitionBy("n_events", "distinct_operators").orderBy("distinct_operators", "n_events").rowsBetween(-1, Window.currentRow)
w_n_events_distinct_operators_2 = Window.partitionBy("n_events", "distinct_operators")

base = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

with_prev = base.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

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
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

rollup_base = (
    session_summary
    .groupBy("n_events")
    .agg(
        F.countDistinct("operator").alias("distinct_operators"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.max("session_unit_price").alias("max_session_unit_price_check"),
    )
    .filter(F.col("max_session_unit_price_check") > 200.9)
    .drop("max_session_unit_price_check")
)

ranked = (
    rollup_base
    .select(
        "*",
        F.round(F.avg("distinct_operators").over(w_n_events_distinct_operators), 2).alias("distinct_operators_avg_2"),
        F.min("distinct_operators").over(w_n_events_distinct_operators_2).alias("n_events_distinct_operators_min_distinct_operato"),
        F.round(F.col("distinct_operators") - F.avg("distinct_operators").over(w_n_events_distinct_operators_2), 2).alias("distinct_operators_vs_n_events_distinct_operator"),
    )
)

result = (
    ranked
    .filter(F.col("distinct_operators_avg_2") <= 6.8)
    .select(
        "n_events",
        "distinct_operators",
        "operator_list",
        "distinct_operators_avg_2",
        "n_events_distinct_operators_min_distinct_operato",
        "distinct_operators_vs_n_events_distinct_operator",
        F.round(F.col("distinct_operators") / F.nullif(F.col("distinct_operators_avg_2"), F.lit(0)), 3).alias("distinct_operators_per_distinct_operators_avg_2"),
    )
    .orderBy(
        F.col("distinct_operators_vs_n_events_distinct_operator").asc_nulls_last(),
        "n_events",
    )
    .limit(5)
)
