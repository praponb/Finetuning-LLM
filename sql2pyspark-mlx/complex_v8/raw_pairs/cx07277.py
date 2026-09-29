from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_moved_at_month = Window.partitionBy("operator", F.month("moved_at"))
w_warehouse_id_sku_id_by_unit = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_sku_id_by_unit_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("unit_price").desc())
w_warehouse_id_by_qty_ties = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc())
w_warehouse_id_by_qty_prior_2 = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), "id").rowsBetween(-2, -1)
w_sku_category_by_moved_at_ful = Window.partitionBy("sku_category").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id = Window.partitionBy("sku_id")
w_sku_id_sku_category_by_unit = Window.partitionBy("sku_id", "sku_category").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-1, Window.currentRow)
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_warehouse_id_by_moved_at_tie = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").asc_nulls_last())
w_warehouse_id_by_qty_moving_2 = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), "id").rowsBetween(-2, Window.currentRow)

recent = (
    df_stock_moves
    .filter((~F.col("operator").isin("op_eve", "op_bee")) & (F.month(F.col("moved_at")).isin(1, 3)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

combined_details = (
    recent.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.array_contains(F.col("sk.labels"), "fragile")), "left")
    .select("s.*", F.col("sk.category").alias("sku_category"))
)

with_windows = (
    combined_details
    .select(
        "*",
        F.round(F.avg("qty").over(w_operator_moved_at_month), 2).alias("operator_moved_at_month_avg_qty"),
        F.last("unit_price").over(w_warehouse_id_sku_id_by_unit).alias("last_unit_price"),
        F.dense_rank().over(w_warehouse_id_sku_id_by_unit_2).alias("unit_price_dense_rank"),
        F.round(F.cume_dist().over(w_warehouse_id_by_qty_ties), 3).alias("qty_cume_dist"),
        F.sum("unit_price").over(w_warehouse_id_by_qty_prior_2).alias("unit_price_sum_2_prior"),
        F.nth_value("qty", 2).over(w_sku_category_by_moved_at_ful).alias("second_qty"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_id), F.lit(0)), 4).alias("qty_share_of_sku_id"),
        F.min("qty").over(w_sku_id).alias("sku_id_min_qty"),
        F.round(F.avg("unit_price").over(w_sku_id_sku_category_by_unit), 2).alias("unit_price_avg_2"),
        F.lead("unit_price").over(w_warehouse_id_by_moved_at).alias("next_unit_price"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator_moved_at_month), F.lit(0)), 4).alias("unit_price_share_of_operator_moved_at_month"),
        F.round(F.percent_rank().over(w_warehouse_id_by_moved_at_tie), 3).alias("moved_at_percent_rank"),
        F.round(F.avg("unit_price").over(w_warehouse_id_by_qty_moving_2), 2).alias("unit_price_avg_3"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "unit_price",
        "sku_category",
        "last_unit_price",
        "qty_cume_dist",
        "unit_price_sum_2_prior",
        "second_qty",
        "qty_share_of_sku_id",
        "sku_id_min_qty",
        "next_unit_price",
        "unit_price_share_of_operator_moved_at_month",
        "moved_at_percent_rank",
    )
    .orderBy(F.col("next_unit_price").desc())
)
