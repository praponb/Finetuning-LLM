from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
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
        F.abs(F.col("qty") - 230).alias("qty_distance"),
    )
)

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_sku_id_running))

enriched = (
    sessionized
    .select(
        "*",
        (F.col("is_new_session") + F.col("unit_price")).alias("is_new_session_plus_unit_price"),
        F.when(F.col("move_type").isin("TRANSFER", "INBOUND"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.round(F.col("is_new_session") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("is_new_session_per_unit_price"),
    )
)

result = (
    enriched
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
        "is_new_session",
        "session_no",
        "is_new_session_plus_unit_price",
        "move_type_group",
        "is_new_session_per_unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
        F.concat_ws(": ", F.col("move_type_group"), F.col("move_type")).alias("move_type_group_move_type_label"),
    )
    .orderBy(F.col("warehouse_id").desc(), "move_type_group_move_type_label")
)
