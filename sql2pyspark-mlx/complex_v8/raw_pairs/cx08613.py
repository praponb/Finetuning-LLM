from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_sku_id_by_warehouse_capacity = Window.partitionBy("sku_id").orderBy(F.col("warehouse_capacity").desc(), "id")
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_all_by_warehouse_capacity_ti = Window.orderBy(F.col("warehouse_capacity").desc())

base = (
    df_stock_moves
    .filter((F.col("operator").isin("op_chan", "op_bee", "op_ann")) & (F.col("qty") < 275))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

with_details_warehouse = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

ranked = (
    with_details_warehouse
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "warehouse_capacity",
        F.round(F.col("qty") - F.avg("qty").over(w_move_type), 2).alias("qty_vs_move_type_avg"),
        F.round(F.percent_rank().over(w_sku_id_by_warehouse_capacity), 3).alias("warehouse_capacity_percent_rank"),
        F.lead("qty").over(w_sku_id_by_warehouse_capacity).alias("next_qty"),
        F.max("warehouse_capacity").over(w_move_type).alias("move_type_max_warehouse_capacity"),
        F.lead("qty").over(w_all_by_qty).alias("next_qty_2"),
        F.row_number().over(w_all_by_qty).alias("seq_no"),
        F.round(F.avg("qty").over(w_warehouse_id_sku_id), 2).alias("warehouse_id_sku_id_avg_qty"),
        F.rank().over(w_all_by_warehouse_capacity_ti).alias("warehouse_capacity_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "sku_id",
        "move_type",
        "next_qty",
        "move_type_max_warehouse_capacity",
        "next_qty_2",
        "seq_no",
        "warehouse_capacity_rank",
        (F.floor(F.col("qty_vs_move_type_avg") / 250) * 250).alias("qty_vs_move_type_avg_bucket"),
    )
    .orderBy(
        F.col("warehouse_capacity_rank").desc_nulls_first(),
        F.col("id").desc_nulls_first(),
    )
)
