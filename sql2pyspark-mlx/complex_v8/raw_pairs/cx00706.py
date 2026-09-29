from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
    .filter(F.col("operator") == "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.when(F.col("qty") >= 415, "HIGH").when(F.col("qty") >= 116, "MEDIUM").otherwise("LOW").alias("qty_tier"),
    )
)

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_sku_id_running))

with_details = (
    sessionized.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.weight_kg") > 8.5), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(~F.col("sk.is_active"))
    .select(
        "s.*",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

scored = (
    with_details
    .filter(F.col("sku_id") <= 4)
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "operator",
        "qty_tier",
        "prev_moved_at",
        "starts_session",
        "sku_is_active",
        "warehouse_opened_on",
        F.when(F.col("starts_session") >= 1, "HIGH").when(F.col("starts_session") >= 0, "MEDIUM").otherwise("LOW").alias("starts_session_tier"),
        F.to_date(F.col("prev_moved_at")).alias("prev_moved_at_date"),
        F.date_format(F.col("prev_moved_at"), "yyyy-MM-dd HH:00").alias("prev_moved_at_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "qty",
        "moved_at",
        "operator",
        "qty_tier",
        "prev_moved_at",
        "sku_is_active",
        "warehouse_opened_on",
        "starts_session_tier",
        "prev_moved_at_date",
        "prev_moved_at_label",
        F.date_trunc("month", F.col("warehouse_opened_on")).alias("warehouse_opened_on_month"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
    .orderBy(F.col("qty_tier").desc())
)
