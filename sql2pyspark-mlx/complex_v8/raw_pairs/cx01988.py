from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_stock_moves
    .filter(F.col("sku_id") >= 6)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

with_prev = valid.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_sku_id_running))

per_session = (
    sessionized
    .groupBy("sku_id", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

summarized_rows = (
    per_session.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
)

summarized_rows_2 = (
    summarized_rows.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.last_moved_at")
)

result = (
    summarized_rows_2
    .filter(F.col("duration_min").between(48.0, 156.0))
    .select(
        "sku_id",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "session_unit_price",
        "move_count",
        F.date_format(F.col("session_end"), "yyyy-MM").alias("session_end_label"),
    )
    .orderBy(F.col("move_count").asc(), F.col("session_end_label").desc())
)
