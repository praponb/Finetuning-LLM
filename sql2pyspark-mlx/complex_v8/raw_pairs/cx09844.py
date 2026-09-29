from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_sku_is_active_by_days = Window.partitionBy("sku_id", "sku_is_active").orderBy("days_since_moved_at", "id").rowsBetween(-1, Window.currentRow)
w_sku_id_sku_is_active_by_days_2 = Window.partitionBy("sku_id", "sku_is_active").orderBy("days_since_moved_at", "id").rowsBetween(-2, Window.currentRow)
w_sku_id_sku_is_active_by_days_3 = Window.partitionBy("sku_id", "sku_is_active").orderBy("days_since_moved_at")
w_sku_id_sku_is_active_by_days_4 = Window.partitionBy("sku_id", "sku_is_active").orderBy("days_since_moved_at", "id")

filtered_moves = (
    df_stock_moves
    .filter((F.col("unit_price") > 161.4) & (F.col("operator") != "op_eve"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.when(F.col("qty") >= 314, "HIGH").when(F.col("qty") >= 112, "MEDIUM").otherwise("LOW").alias("qty_tier"),
    )
)

combined_details_warehouse = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.days_since_moved_at",
        "s.qty_tier",
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_trends = (
    combined_details_warehouse
    .select(
        "*",
        F.round(F.avg("days_since_moved_at").over(w_sku_id_sku_is_active_by_days), 2).alias("days_since_moved_at_avg_2"),
        F.sum("days_since_moved_at").over(w_sku_id_sku_is_active_by_days_2).alias("days_since_moved_at_sum_3"),
        F.round(F.cume_dist().over(w_sku_id_sku_is_active_by_days_3), 3).alias("days_since_moved_at_cume_dist"),
        (F.col("days_since_moved_at") - F.lag("days_since_moved_at").over(w_sku_id_sku_is_active_by_days_4)).alias("days_since_moved_at_change"),
    )
)

result = (
    with_trends
    .filter(F.col("days_since_moved_at_change") >= -210)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "days_since_moved_at",
        "qty_tier",
        "warehouse_city",
        "sku_labels",
        "days_since_moved_at_avg_2",
        "days_since_moved_at_sum_3",
        F.when(F.col("days_since_moved_at") >= 560, "HIGH").when(F.col("days_since_moved_at") >= 175, "MEDIUM").otherwise("LOW").alias("days_since_moved_at_tier"),
        F.when(F.col("sku_is_active"), 1).otherwise(0).alias("sku_is_active_flag"),
    )
    .orderBy(F.col("warehouse_city").desc())
)
