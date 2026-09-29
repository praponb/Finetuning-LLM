from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_moves = (
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
    )
)

with_prev = scoped_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_move_type_running))

per_session = (
    sessionized
    .groupBy("move_type", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    per_session
    .filter(F.hour(F.col("session_end")).between(9, 15))
    .select(
        "move_type",
        "burst_id",
        "session_start",
        "events",
        "duration_min",
        F.when(F.col("move_type").isin("TRANSFER", "ADJUST"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.when(F.col("duration_min") >= 156.0, "A").when(F.col("duration_min") >= 96.0, "B").otherwise("C").alias("duration_min_tier"),
        F.round(F.col("duration_min") * 0.9, 2).alias("duration_min_adj"),
    )
)

result = (
    labeled
    .select(
        "move_type",
        "burst_id",
        "session_start",
        "move_type_group",
        "duration_min_tier",
        "duration_min_adj",
    )
    .orderBy(F.col("burst_id").desc(), "duration_min_tier", "move_type")
    .limit(10)
)
