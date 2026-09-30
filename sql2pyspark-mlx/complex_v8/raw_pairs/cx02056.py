from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stock_moves
    .filter(F.col("qty").between(90, 384))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_move_type_running))

result = (
    sessionized
    .filter(F.col("sku_id") >= 10)
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "unit_price",
        "qty_minus_unit_price",
        "prev_moved_at",
        "session_id",
        F.round(F.col("unit_price") / F.nullif(F.col("is_new_session"), F.lit(0)), 3).alias("unit_price_per_is_new_session"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
    .orderBy(F.col("days_since_moved_at").desc())
)
