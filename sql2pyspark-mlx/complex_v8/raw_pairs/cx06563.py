from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_2 = Window.partitionBy("operator")
w_session_no_by_session_unit_p = Window.partitionBy("session_no").orderBy("session_unit_price_bucket", "operator", "session_no").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_stock_moves
    .filter(F.col("move_type") != "ADJUST")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

with_prev = scoped.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_operator_running))

per_session = (
    sessionized
    .groupBy("operator", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("unit_price_bucket").alias("session_unit_price_bucket"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    per_session
    .select(
        "*",
        F.count(F.lit(1)).over(w_operator_2).alias("rows_in_operator"),
        F.nth_value("session_start", 2).over(w_session_no_by_session_unit_p).alias("second_session_start"),
        F.round(F.col("session_no") / F.nullif(F.sum("session_no").over(w_operator_2), F.lit(0)), 4).alias("session_no_share_of_operator"),
        F.max("duration_min").over(w_operator_2).alias("operator_max_duration_min"),
    )
)

derived = (
    sequenced
    .filter(F.col("session_unit_price_bucket") >= 401)
    .select("*", F.lower(F.trim(F.col("operator"))).alias("operator_clean"))
)

result = (
    derived
    .select(
        "operator",
        "session_no",
        "session_end",
        "events",
        "session_unit_price_bucket",
        "duration_min",
        "rows_in_operator",
        "session_no_share_of_operator",
        "operator_max_duration_min",
        "operator_clean",
        F.to_date(F.col("session_end")).alias("session_end_date"),
    )
)
