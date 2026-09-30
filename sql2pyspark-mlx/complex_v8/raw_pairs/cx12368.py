from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = df_stock_moves

with_prev = valid.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_operator_running))

enriched = (
    sessionized.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category") != "Electronics"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.prev_moved_at",
        "s.session_no",
        F.col("sk.is_active").alias("sku_is_active"),
        "sk.sku_code",
        F.col("sk.category").alias("sku_category"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

result = (
    enriched
    .filter(F.col("warehouse_city") != "Kuala Lumpur")
    .select(
        "id",
        "move_type",
        "operator",
        "session_no",
        "sku_is_active",
        "sku_code",
        "sku_category",
        "warehouse_city",
        F.when(F.col("sku_is_active"), "yes").otherwise("no").alias("sku_is_active_label"),
        F.substring(F.trim(F.col("warehouse_city")), 1, 4).alias("warehouse_city_prefix"),
    )
)
