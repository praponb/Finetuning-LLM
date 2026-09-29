from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(8, 13))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.when(F.col("move_type").isin("TRANSFER", "OUTBOUND"), "group_a").when(F.col("move_type") == "ADJUST", "group_b").otherwise("other").alias("move_type_group"),
    )
)

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_operator_running))

sessions = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .select(
        "operator",
        "session_id",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
    .orderBy("operator")
)
