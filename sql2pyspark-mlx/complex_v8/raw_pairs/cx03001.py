from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

with_prev = filtered_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_move_type_running))

result = (
    sessionized
    .rollup("move_type", "session_no", "warehouse_id")
    .agg(F.max("qty").alias("max_qty"))
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("session_no").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
