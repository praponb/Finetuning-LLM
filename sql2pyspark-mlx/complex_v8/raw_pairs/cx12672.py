from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("warehouse_id").orderBy(F.col("new_session_flag").desc(), F.col("id").desc())

valid = (
    df_stock_moves
    .filter(F.col("qty") >= 189)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.year(F.col("moved_at")).alias("moved_at_year"),
    )
)

with_prev = valid.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_operator_running))

latest = (
    sessionized
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "session_id",
    )
    .orderBy("qty", F.col("move_type").desc(), "id")
    .limit(20)
)
