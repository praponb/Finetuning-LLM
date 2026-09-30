from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_moves = (
    df_stock_moves
    .filter(F.col("move_type") != "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

with_prev = prepared_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_sku_id_running))

without_matches = (
    sessionized.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.size(F.col("sk.labels")) >= 1) & (F.col("sk.unit_cost") < 320.4), "left_semi")
)

result = (
    without_matches
    .groupBy("prev_moved_at", "sku_id")
    .pivot("operator", ["op_dew", "op_eve"])
    .agg(F.sum("unit_price"))
    .orderBy("prev_moved_at", "sku_id")
)
