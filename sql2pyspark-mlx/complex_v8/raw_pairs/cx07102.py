from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
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

with_prev = valid.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_operator_running))

result = (
    sessionized
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "prev_moved_at",
        "new_session_flag",
        F.concat(F.col("operator"), F.lit("#"), F.col("session_no").cast("string")).alias("operator_ref"),
    )
    .orderBy(F.col("move_type").desc(), "id")
    .limit(20)
)
