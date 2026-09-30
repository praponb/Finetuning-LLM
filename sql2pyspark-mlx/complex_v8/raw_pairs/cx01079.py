from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_sku_id = Window.partitionBy("move_type", "sku_id")
w_warehouse_id_by_days_since_m = Window.partitionBy("warehouse_id").orderBy(F.col("days_since_moved_at").asc_nulls_last(), "id")

base = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

metrics = (
    base
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "days_since_moved_at",
        "qty_per_unit_price",
        F.round(F.col("qty_per_unit_price") / F.nullif(F.sum("qty_per_unit_price").over(w_move_type_sku_id), F.lit(0)), 4).alias("qty_per_unit_price_share_of_move_type_sku_id"),
        F.ntile(10).over(w_warehouse_id_by_days_since_m).alias("days_since_moved_at_decile"),
        F.size(F.collect_set("operator").over(w_move_type_sku_id)).alias("distinct_operator_in_move_type_sku_id"),
        F.round(F.avg("days_since_moved_at").over(w_move_type_sku_id), 2).alias("move_type_sku_id_avg_days_since_moved_at"),
    )
)

tagged = (
    metrics
    .filter(F.hour(F.col("moved_at")).between(7, 9))
    .select(
        "id",
        "qty",
        "moved_at",
        "qty_per_unit_price_share_of_move_type_sku_id",
        "days_since_moved_at_decile",
        "move_type_sku_id_avg_days_since_moved_at",
        F.when(F.col("days_since_moved_at") >= 525, "HIGH").when(F.col("days_since_moved_at") >= 175, "MEDIUM").otherwise("LOW").alias("days_since_moved_at_tier"),
    )
)

result = (
    tagged
    .select(
        "id",
        "qty",
        "moved_at",
        "days_since_moved_at_decile",
        "move_type_sku_id_avg_days_since_moved_at",
        "days_since_moved_at_tier",
    )
    .orderBy(F.col("move_type_sku_id_avg_days_since_moved_at").asc_nulls_last())
)
