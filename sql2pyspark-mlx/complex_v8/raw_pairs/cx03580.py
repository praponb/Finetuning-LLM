from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_operator_by_qty = Window.partitionBy("move_type", "operator").orderBy("qty", "id")
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")

filtered = (
    df_stock_moves
    .filter(F.col("unit_price") < 387.1)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.row_number().over(w_move_type_operator_by_qty).alias("rn"),
        F.sum("unit_price").over(w_operator_warehouse_id).alias("operator_warehouse_id_sum_unit_price"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator_warehouse_id), F.lit(0)), 4).alias("qty_share_of_operator_warehouse_id"),
    )
)

result = (
    windowed
    .groupBy("warehouse_id")
    .pivot("move_type", ["OUTBOUND", "TRANSFER", "ADJUST", "INBOUND"])
    .agg(F.avg("operator_warehouse_id_sum_unit_price"))
    .orderBy("warehouse_id")
)
