from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_warehouse_id_by_moved_at_tie = Window.partitionBy("warehouse_id").orderBy("moved_at")
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_operator_by_mov = Window.partitionBy("warehouse_id", "operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_warehouse_id_operator_by_mov_2 = Window.partitionBy("warehouse_id", "operator").orderBy("moved_at", "id").rowsBetween(-2, -1)

filtered_moves = (
    df_stock_moves
    .filter((F.col("qty") < 355) & (F.col("unit_price") <= 367.6))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
    )
)

windowed = (
    filtered_moves
    .select(
        "*",
        F.row_number().over(w_all_by_unit_price).alias("unit_price_seq"),
        F.rank().over(w_warehouse_id_by_moved_at_tie).alias("moved_at_rank"),
        F.round(F.percent_rank().over(w_warehouse_id_by_moved_at), 3).alias("moved_at_percent_rank"),
        F.lead("moved_at").over(w_all_by_unit_price).alias("next_moved_at"),
        F.first("unit_price").over(w_warehouse_id_operator_by_mov).alias("first_unit_price"),
        (F.col("unit_price") - F.lag("unit_price").over(w_warehouse_id_by_moved_at)).alias("unit_price_change"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_operator_warehouse_id"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id), 2).alias("unit_price_vs_warehouse_id_avg"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_operator_warehouse_id), 2).alias("unit_price_vs_operator_warehouse_id_avg"),
        F.dense_rank().over(w_all_by_unit_price_ties).alias("unit_price_dense_rank"),
        F.round(F.percent_rank().over(w_warehouse_id_by_moved_at_tie), 3).alias("moved_at_percent_rank_2"),
        F.max("unit_price").over(w_warehouse_id_operator_by_mov_2).alias("unit_price_max_2_prior"),
    )
)

result = (
    windowed
    .select(
        "id",
        "operator",
        "unit_price",
        "unit_price_seq",
        "moved_at_percent_rank",
        "next_moved_at",
        "first_unit_price",
        "unit_price_change",
        "unit_price_vs_warehouse_id_avg",
        "unit_price_vs_operator_warehouse_id_avg",
        "moved_at_percent_rank_2",
        "unit_price_max_2_prior",
        (F.col("unit_price_seq") + F.col("moved_at_percent_rank_2")).alias("unit_price_seq_plus_moved_at_percent_rank_2"),
    )
    .orderBy(F.col("id").desc(), F.col("unit_price_change").desc())
)
