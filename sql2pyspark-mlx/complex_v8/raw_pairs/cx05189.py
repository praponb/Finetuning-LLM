from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id_by_qty = Window.partitionBy("sku_id", "warehouse_id").orderBy("qty", "id")
w_sku_id_warehouse_id_by_qty_t = Window.partitionBy("sku_id", "warehouse_id").orderBy("qty")
w_sku_id = Window.partitionBy("sku_id")
w_sku_is_active = Window.partitionBy("sku_is_active")
w_operator_warehouse_id_by_qty = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("qty_cume_dist").desc())
w_warehouse_id_warehouse_opene = Window.partitionBy("warehouse_id", F.month("warehouse_opened_on")).orderBy("warehouse_opened_on", "id")

scoped_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
    )
)

metrics = (
    scoped_moves
    .select(
        "*",
        F.row_number().over(w_sku_id_warehouse_id_by_qty).alias("rn"),
        F.round(F.cume_dist().over(w_sku_id_warehouse_id_by_qty_t), 3).alias("qty_cume_dist"),
        F.rank().over(w_sku_id_warehouse_id_by_qty_t).alias("qty_rank"),
        F.sum("unit_price").over(w_sku_id).alias("sku_id_sum_unit_price"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
    )
)

with_details = (
    metrics.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.move_type_prefix",
        "s.rn",
        "s.qty_cume_dist",
        "s.qty_rank",
        "s.sku_id_sum_unit_price",
        "s.sku_id_avg_qty",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

metrics_2 = (
    with_details
    .select(
        "*",
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_sku_is_active), 2).alias("warehouse_capacity_vs_sku_is_active_avg"),
        F.count(F.lit(1)).over(w_sku_is_active).alias("rows_in_sku_is_active"),
    )
)

metrics_3 = (
    metrics_2
    .select(
        "*",
        F.round(F.col("qty_cume_dist") / F.nullif(F.sum("qty_cume_dist").over(w_sku_is_active), F.lit(0)), 4).alias("qty_cume_dist_share_of_sku_is_active"),
        F.round(F.col("qty_rank") / F.nullif(F.sum("qty_rank").over(w_sku_is_active), F.lit(0)), 4).alias("qty_rank_share_of_sku_is_active"),
        F.round(F.cume_dist().over(w_operator_warehouse_id_by_qty), 3).alias("qty_cume_dist_cume_dist"),
        F.lag("qty", 3).over(w_warehouse_id_warehouse_opene).alias("prev3_qty"),
    )
)

result = (
    metrics_3
    .groupBy("warehouse_id", "qty_rank")
    .pivot("operator", ["op_bee", "op_chan"])
    .agg(F.count("rows_in_sku_is_active"))
    .orderBy("warehouse_id", "qty_rank")
)
