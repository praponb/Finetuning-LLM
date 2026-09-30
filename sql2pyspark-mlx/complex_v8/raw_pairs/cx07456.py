from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
    )
)

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_operator_running))

annotated = (
    sessionized
    .filter(F.col("unit_price_adj") <= 487.9)
    .withColumn("unit_price_adj_plus_qty", F.col("unit_price_adj") + F.col("qty"))
)

result = (
    annotated
    .filter(F.col("unit_price_adj") <= 562.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price_adj",
        "prev_moved_at",
        "new_session_flag",
        "unit_price_adj_plus_qty",
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
    )
    .orderBy(F.col("new_session_flag").desc_nulls_first())
)
