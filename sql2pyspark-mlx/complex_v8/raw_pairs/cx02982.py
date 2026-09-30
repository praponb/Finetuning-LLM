from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_sku_id_running))

sessions = (
    sessionized
    .groupBy("sku_id", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

with_parent_sku = (
    sessions.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.weight_kg") >= 6.4), "inner")
    .select("s.*", F.col("sk.weight_kg").alias("sku_weight_kg"))
)

annotated = (
    with_parent_sku
    .filter(F.col("sku_id") <= 10)
    .select(
        "*",
        (F.col("session_no") + F.col("session_qty")).alias("session_no_plus_session_qty"),
        F.round(F.col("duration_min") * 0.9, 2).alias("duration_min_adj"),
    )
)

result = (
    annotated
    .select(
        "sku_id",
        "session_no",
        "session_start",
        "event_count",
        "session_qty",
        "duration_min",
        "sku_weight_kg",
        "session_no_plus_session_qty",
        "duration_min_adj",
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 60, 1).alias("minutes_session_end_to_session_start"),
        F.round(F.col("sku_weight_kg") * 1.1, 2).alias("sku_weight_kg_adj"),
    )
    .orderBy(
        F.col("session_no_plus_session_qty").desc(),
        F.col("session_start").desc(),
    )
)
