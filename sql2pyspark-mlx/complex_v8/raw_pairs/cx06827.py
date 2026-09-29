from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator_by_mov = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("moved_at").desc(), "id")
w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_operator = Window.partitionBy("operator")
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_operator_moved_at_year_by_da = Window.partitionBy("operator", F.year("moved_at")).orderBy("days_since_moved_at")
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_move_type_operator_by_moved = Window.partitionBy("move_type", "operator").orderBy(F.col("moved_at").desc(), "id")
w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())

prepared_moves = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("qty") <= 256))
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
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

with_trends = (
    prepared_moves
    .select(
        "*",
        F.round(F.percent_rank().over(w_warehouse_id_operator_by_mov), 3).alias("moved_at_percent_rank"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator_sku_id), F.lit(0)), 4).alias("unit_price_share_of_operator_sku_id"),
        F.round(F.col("days_since_moved_at") / F.nullif(F.sum("days_since_moved_at").over(w_operator), F.lit(0)), 4).alias("days_since_moved_at_share_of_operator"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_qty)).alias("unit_price_change"),
        F.dense_rank().over(w_operator_moved_at_year_by_da).alias("days_since_moved_at_dense_rank"),
        F.round(F.avg("unit_price").over(w_operator_sku_id), 2).alias("operator_sku_id_avg_unit_price"),
        F.sum("days_since_moved_at").over(w_operator_sku_id).alias("operator_sku_id_sum_days_since_moved_at"),
        F.round(F.col("days_since_moved_at") - F.avg("days_since_moved_at").over(w_warehouse_id_move_type), 2).alias("days_since_moved_at_vs_warehouse_id_move_type_av"),
        F.lag("days_since_moved_at", 3).over(w_move_type_operator_by_moved).alias("prev3_days_since_moved_at"),
        F.round(F.cume_dist().over(w_all_by_qty_ties), 3).alias("qty_cume_dist"),
        F.round(F.col("days_since_moved_at") - F.avg("days_since_moved_at").over(w_operator_sku_id), 2).alias("days_since_moved_at_vs_operator_sku_id_avg"),
        F.lag("days_since_moved_at").over(w_all_by_qty).alias("prev_days_since_moved_at"),
        F.round(F.cume_dist().over(w_operator_moved_at_year_by_da), 3).alias("days_since_moved_at_cume_dist"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "moved_at_label",
        "days_since_moved_at",
        "moved_at_percent_rank",
        "days_since_moved_at_share_of_operator",
        "unit_price_change",
        "days_since_moved_at_dense_rank",
        "operator_sku_id_sum_days_since_moved_at",
        "days_since_moved_at_vs_warehouse_id_move_type_av",
        "days_since_moved_at_cume_dist",
        F.concat_ws("-", F.col("moved_at_label"), F.col("move_type")).alias("moved_at_label_move_type_label"),
    )
    .orderBy(F.col("days_since_moved_at_dense_rank").desc())
)
