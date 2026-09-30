from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
    )
)

with_prev = prepared.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_move_type_running))

session_summary = (
    sessionized
    .groupBy("move_type", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

per_group = (
    session_summary
    .groupBy(
        "move_type",
        F.date_format("session_end", "yyyy-MM").alias("session_end_ym"),
        F.date_trunc("month", "session_start").alias("session_start_month"),
    )
    .agg(
        F.min("session_end").alias("first_session_end"),
        F.max("event_count").alias("max_event_count"),
        F.round(F.sum("session_no") / F.nullif(F.sum("event_count"), F.lit(0)), 3).alias("session_no_to_event_count_ratio"),
        F.count(F.when(F.col("move_type") != "TRANSFER", 1)).alias("n_move_type_transfer"),
    )
)

result = (
    per_group
    .filter(F.col("n_move_type_transfer") > 1)
    .select(
        "move_type",
        "first_session_end",
        "max_event_count",
        "session_no_to_event_count_ratio",
        F.when(F.col("move_type").isin("OUTBOUND", "TRANSFER"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
        (F.col("max_event_count") + F.col("n_move_type_transfer")).alias("max_event_count_plus_n_move_type_transfer"),
    )
    .orderBy(F.col("max_event_count").desc_nulls_first())
)
