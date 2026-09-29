from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp"))
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

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_sku_id_running))

derived = (
    sessionized
    .filter(F.col("sku_id") <= 4)
    .withColumn("move_type_ref", F.concat(F.col("move_type"), F.lit("#"), F.col("id").cast("string")))
)

result = (
    derived
    .filter(F.col("move_type").isin("TRANSFER", "INBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "prev_moved_at",
        "new_session_flag",
    )
    .orderBy(F.col("moved_at").asc_nulls_last(), F.col("new_session_flag").desc())
)
