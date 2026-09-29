from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_move_type = Window.partitionBy("move_type")
w_all_by_unit_price_prior_6 = Window.orderBy("unit_price", "id").rowsBetween(-6, -1)
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_operator_sku_id_by_qty_ties = Window.partitionBy("operator", "sku_id").orderBy(F.col("qty").desc())
w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())
w_operator_sku_id_by_qty_full = Window.partitionBy("operator", "sku_id").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_qty_prior_1 = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(-1, -1)
w_all_by_qty_moving_2 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-2, Window.currentRow)

prepared = (
    df_stock_moves
    .filter((F.col("qty") >= 99) & (F.col("sku_id").isNotNull()))
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

sequenced = (
    prepared
    .select(
        "*",
        F.round(F.col("qty") - F.avg("qty").over(w_operator), 2).alias("qty_vs_operator_avg"),
        F.round(F.avg("qty").over(w_move_type), 2).alias("move_type_avg_qty"),
        F.max("unit_price").over(w_all_by_unit_price_prior_6).alias("unit_price_max_6_prior"),
        F.rank().over(w_all_by_qty).alias("qty_rank"),
        (F.col("qty") - F.lag("qty").over(w_all_by_unit_price)).alias("qty_change"),
        F.round(F.cume_dist().over(w_all_by_qty), 3).alias("qty_cume_dist"),
        F.lag("qty", 1, 0).over(w_all_by_unit_price).alias("prev_qty"),
        F.rank().over(w_operator_sku_id_by_qty_ties).alias("qty_rank_2"),
        F.rank().over(w_all_by_qty_ties).alias("qty_rank_3"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_operator), 2).alias("unit_price_vs_operator_avg"),
        F.first("qty").over(w_operator_sku_id_by_qty_full).alias("first_qty"),
        F.round(F.avg("qty").over(w_warehouse_id_by_qty_prior_1), 2).alias("qty_avg_1_prior"),
        F.round(F.avg("unit_price").over(w_move_type), 2).alias("move_type_avg_unit_price"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type), F.lit(0)), 4).alias("unit_price_share_of_move_type"),
        F.round(F.avg("qty").over(w_all_by_qty_moving_2), 2).alias("qty_avg_3"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "move_type_avg_qty",
        "unit_price_max_6_prior",
        "qty_rank",
        "qty_change",
        "qty_cume_dist",
        "qty_rank_3",
        "unit_price_vs_operator_avg",
        "first_qty",
        "qty_avg_1_prior",
        "move_type_avg_unit_price",
        "unit_price_share_of_move_type",
        "qty_avg_3",
        "rows_in_move_type",
        F.abs(F.col("unit_price") - 180.0).alias("unit_price_distance"),
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)
