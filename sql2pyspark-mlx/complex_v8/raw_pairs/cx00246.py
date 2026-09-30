from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_all_by_qty_full = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_moves = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-02-12").cast("date")) & (F.col("unit_price") < 399.6))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

metrics = (
    filtered_moves
    .select(
        "*",
        F.round(F.col("moved_at_dayofweek") - F.avg("moved_at_dayofweek").over(w_warehouse_id_move_type), 2).alias("moved_at_dayofweek_vs_warehouse_id_move_type_avg"),
        F.first("qty").over(w_all_by_qty_full).alias("first_qty"),
    )
)

combined_details_sku = (
    metrics.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.moved_at_dayofweek",
        "s.first_qty",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

result = (
    combined_details_sku
    .groupBy("sku_id")
    .pivot("move_type", ["INBOUND", "TRANSFER"])
    .agg(F.count("moved_at_dayofweek").alias("count_v"), F.sum("moved_at_dayofweek").alias("sum_v"))
    .orderBy("sku_id")
)
