from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_move_type = Window.partitionBy("operator", "move_type")
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_sku_id_operator_by_moved_at = Window.partitionBy("sku_id", "operator").orderBy("moved_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_sku_id_by_unit_price_full = Window.partitionBy("sku_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_operator_by_moved_at_2 = Window.partitionBy("sku_id", "operator").orderBy("moved_at_day", "id").rowsBetween(-2, -1)

cleaned = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator_move_type), F.lit(0)), 4).alias("unit_price_share_of_operator_move_type"),
        F.rank().over(w_all_by_unit_price_ties).alias("unit_price_rank"),
        F.nth_value("unit_price", 2).over(w_sku_id_operator_by_moved_at).alias("second_unit_price"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_warehouse_id"),
        F.round(F.avg("qty").over(w_warehouse_id), 2).alias("warehouse_id_avg_qty"),
        F.ntile(5).over(w_warehouse_id_by_qty).alias("qty_quintile"),
        F.max("unit_price").over(w_warehouse_id).alias("warehouse_id_max_unit_price"),
        F.lag("unit_price", 3, 0).over(w_all_by_unit_price).alias("prev3_unit_price"),
        F.nth_value("qty", 2).over(w_sku_id_by_unit_price_full).alias("second_qty"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator_move_type), F.lit(0)), 4).alias("qty_share_of_operator_move_type"),
        F.sum("qty").over(w_sku_id_operator_by_moved_at_2).alias("qty_sum_2_prior"),
    )
)

top_per_group = (
    sequenced
    .filter(F.col("unit_price_rank") == 1)
    .drop("unit_price_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "moved_at_day",
        "unit_price_share_of_operator_move_type",
        "second_unit_price",
        "unit_price_share_of_warehouse_id",
        "warehouse_id_avg_qty",
        "qty_quintile",
        "warehouse_id_max_unit_price",
        "prev3_unit_price",
        "second_qty",
    )
    .orderBy(F.col("warehouse_id_avg_qty").desc(), "second_unit_price")
)
