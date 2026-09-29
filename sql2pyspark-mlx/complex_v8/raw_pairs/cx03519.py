from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_stock_moves
    .filter(F.col("operator").isin("op_dew", "op_eve", "op_ann"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

with_prev = recent.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_move_type_running))

result = (
    sessionized
    .filter(F.col("operator").isin("op_dew", "op_eve", "op_chan"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "move_type_clean",
        "starts_session",
        F.substring(F.trim(F.col("move_type_clean")), 1, 4).alias("move_type_clean_prefix"),
    )
    .orderBy(
        F.col("id").desc_nulls_first(),
        F.col("warehouse_id").desc_nulls_first(),
    )
)
