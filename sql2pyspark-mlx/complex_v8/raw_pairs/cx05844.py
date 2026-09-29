from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator_by_day = Window.partitionBy("warehouse_id", "operator").orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_operator_by_day_2 = Window.partitionBy("warehouse_id", "operator").orderBy("days_since_moved_at", "id").rowsBetween(-6, Window.currentRow)
w_sku_id_operator = Window.partitionBy("sku_id", "operator")

filtered = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(10, 14))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.last("unit_price_adj", ignorenulls=True).over(w_warehouse_id_operator_by_day).alias("last_known_unit_price_adj"),
        F.max("unit_price_adj").over(w_warehouse_id_operator_by_day_2).alias("unit_price_adj_max_7"),
        F.max("qty").over(w_sku_id_operator).alias("sku_id_operator_max_qty"),
    )
)

result = (
    ranked
    .groupBy("operator")
    .pivot("move_type", ["INBOUND", "ADJUST", "TRANSFER"])
    .agg(F.min("unit_price").alias("min_v"), F.count("unit_price").alias("n"))
    .orderBy("operator")
)
