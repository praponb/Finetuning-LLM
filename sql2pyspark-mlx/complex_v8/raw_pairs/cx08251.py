from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

with_prev = scoped_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_sku_id_running))

result = (
    sessionized
    .select(
        "id",
        "sku_id",
        "qty",
        "unit_price",
        "qty_bucket",
        "prev_moved_at",
        "new_session_flag",
        F.round(F.col("unit_price") / F.nullif(F.col("session_no"), F.lit(0)), 3).alias("unit_price_per_session_no"),
    )
)
