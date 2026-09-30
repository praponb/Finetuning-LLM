from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator_by_qty = Window.partitionBy("warehouse_id", "operator").orderBy("qty_filled")
w_operator = Window.partitionBy("operator")

prepared = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.round(F.cume_dist().over(w_warehouse_id_operator_by_qty), 3).alias("qty_filled_cume_dist"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
    )
)

summary = (
    ranked
    .groupBy(F.to_date("moved_at").alias("moved_at_day"), "warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.sum(F.when(F.col("operator").isin("op_chan", "op_eve"), 1).otherwise(0)).alias("n_operator_in_op_chan_op_eve"),
        F.sum("qty_filled_cume_dist").alias("total_qty_filled_cume_dist"),
        F.round(F.avg("qty_filled"), 2).alias("avg_qty_filled"),
    )
)

joined = (
    summary.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (~F.col("w.city").isin("Singapore", "Kuala Lumpur")), "inner")
    .select(
        "s.moved_at_day",
        "s.warehouse_id",
        "s.n_operator_in_op_chan_op_eve",
        "s.total_qty_filled_cume_dist",
        "s.avg_qty_filled",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    joined
    .groupBy("n_operator_in_op_chan_op_eve", "moved_at_day")
    .pivot("warehouse_city", ["Bangkok", "Singapore"])
    .agg(F.max("total_qty_filled_cume_dist").alias("max_v"), F.count("total_qty_filled_cume_dist").alias("n"))
    .orderBy("n_operator_in_op_chan_op_eve", "moved_at_day")
)
