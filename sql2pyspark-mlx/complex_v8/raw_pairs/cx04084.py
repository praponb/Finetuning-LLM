from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_stock_moves
    .filter(F.col("move_type") != "TRANSFER")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
    )
)

with_prev = scoped.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_sku_id_running))

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.sum("qty").alias("total_move_qty"),
        F.count("*").alias("move_count"),
    )
)

profiled = (
    sessionized.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        "st.total_move_qty",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "move_type_prefix",
        "prev_moved_at",
        "session_no",
        "last_moved_at",
        "total_move_qty",
        F.to_date(F.col("last_moved_at")).alias("last_moved_at_date"),
        F.greatest(F.col("move_count"), F.col("session_no")).alias("greatest_move_count_session_no"),
    )
    .orderBy("unit_price")
)
