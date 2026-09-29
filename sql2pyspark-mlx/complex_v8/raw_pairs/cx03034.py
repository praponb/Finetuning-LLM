from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_stock_moves
    .filter(F.col("qty") > 111)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
    )
)

with_prev = prepared.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_operator_running))

result = (
    sessionized
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "moved_at_week",
        "starts_session",
        "session_id",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
    .orderBy(F.col("qty").asc_nulls_last())
)
