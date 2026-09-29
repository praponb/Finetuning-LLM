from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_sku_unit_cost = Window.partitionBy("sku_id").orderBy("sku_unit_cost", "id")
w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_operator_by_qty = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("qty").asc_nulls_last(), "id")
w_warehouse_id_operator_by_qty_2 = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("qty").asc_nulls_last())
w_warehouse_id_operator_by_qty_3 = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_unit_price_full = Window.partitionBy("move_type").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_moves = (
    df_stock_moves
    .filter((F.col("qty") <= 387) & (F.col("unit_price") < 292.3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

joined_sku = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select("s.*", F.col("sk.unit_cost").alias("sku_unit_cost"))
)

with_windows = (
    joined_sku
    .select(
        "*",
        F.dense_rank().over(w_sku_id_by_sku_unit_cost).alias("sku_unit_cost_dense_rank"),
        F.size(F.collect_set("operator").over(w_sku_id)).alias("distinct_operator_in_sku_id"),
        (F.col("unit_price_bucket") - F.lag("unit_price_bucket").over(w_sku_id_by_sku_unit_cost)).alias("unit_price_bucket_change"),
        F.row_number().over(w_warehouse_id_operator_by_qty).alias("qty_seq"),
        F.rank().over(w_warehouse_id_operator_by_qty).alias("qty_rank"),
        F.sum("qty").over(w_sku_id).alias("sku_id_sum_qty"),
        F.round(F.cume_dist().over(w_warehouse_id_operator_by_qty_2), 3).alias("qty_cume_dist"),
        F.first("moved_at", ignorenulls=True).over(w_warehouse_id_operator_by_qty_3).alias("first_known_moved_at"),
        F.round(F.col("unit_price_bucket") / F.nullif(F.sum("unit_price_bucket").over(w_sku_id), F.lit(0)), 4).alias("unit_price_bucket_share_of_sku_id"),
        F.last("qty").over(w_move_type_by_unit_price_full).alias("last_qty"),
    )
)

top_rows = with_windows.filter(F.col("sku_unit_cost_dense_rank") <= 2)

result = (
    top_rows
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "unit_price_bucket",
        "sku_unit_cost",
        "sku_unit_cost_dense_rank",
        "distinct_operator_in_sku_id",
        "unit_price_bucket_change",
        "qty_seq",
        "qty_rank",
        "sku_id_sum_qty",
        "unit_price_bucket_share_of_sku_id",
        "last_qty",
        F.concat_ws("-", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.quarter(F.col("first_known_moved_at")).alias("first_known_moved_at_quarter"),
    )
    .orderBy("first_known_moved_at_quarter", "id")
    .limit(20)
)
