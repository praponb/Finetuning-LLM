from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_opened_on_month_op = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on"), "operator").orderBy("warehouse_opened_on", "id")
w_warehouse_opened_on_month_op_2 = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on"), "operator").orderBy("warehouse_opened_on", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
    )
)

joined_warehouse = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.moved_at_plus_7d",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        "sk.sku_code",
    )
)

with_windows = (
    joined_warehouse
    .select(
        "*",
        (F.col("sku_unit_cost") - F.lag("sku_unit_cost").over(w_warehouse_opened_on_month_op)).alias("sku_unit_cost_change"),
        F.round(F.avg("sku_unit_cost").over(w_warehouse_opened_on_month_op_2), 2).alias("sku_unit_cost_avg_3"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["operator", "move_type", "sku_id"], ["operator"]],
        "operator", "move_type", "sku_id",
    )
    .agg(
        F.round(F.sum("sku_unit_cost_change") / F.nullif(F.sum("unit_price"), F.lit(0)), 3).alias("sku_unit_cost_change_to_unit_price_ratio"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
    )
    .orderBy(
        F.col("operator").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
