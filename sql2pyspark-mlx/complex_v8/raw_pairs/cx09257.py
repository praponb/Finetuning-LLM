from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_unit_price_full = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_qty_bucket_full = Window.partitionBy("sku_id").orderBy(F.col("qty_bucket").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_warehouse_id_by_qty = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("qty").desc(), "id")
w_moved_at_month_by_moved_at = Window.partitionBy(F.month("moved_at")).orderBy("moved_at", "id")
w_unit_price_dense_rank_wareho = Window.partitionBy("unit_price_dense_rank", "warehouse_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_month_by_moved_at_t = Window.partitionBy(F.month("moved_at")).orderBy("moved_at")

cleaned_moves = (
    df_stock_moves
    .filter((F.col("move_type") == "TRANSFER") & (F.col("unit_price") < 277.6))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

with_windows = (
    cleaned_moves
    .select(
        "*",
        F.dense_rank().over(w_all_by_unit_price).alias("unit_price_dense_rank"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.nth_value("qty", 2).over(w_all_by_unit_price_full).alias("second_qty"),
        F.first("qty_bucket").over(w_sku_id_by_qty_bucket_full).alias("first_qty_bucket"),
        F.sum("qty").over(w_warehouse_id).alias("warehouse_id_sum_qty"),
        F.lag("unit_price", 3).over(w_operator_warehouse_id_by_qty).alias("prev3_unit_price"),
    )
)

top_rows = with_windows.filter(F.col("unit_price_dense_rank") == 1)

sequenced = (
    top_rows
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "operator",
        "unit_price",
        "qty_bucket",
        "rows_in_warehouse_id",
        "second_qty",
        "first_qty_bucket",
        "prev3_unit_price",
        F.ntile(3).over(w_moved_at_month_by_moved_at).alias("moved_at_tercile"),
        F.last("qty_bucket", ignorenulls=True).over(w_unit_price_dense_rank_wareho).alias("last_known_qty_bucket"),
        F.last("prev3_unit_price", ignorenulls=True).over(w_unit_price_dense_rank_wareho).alias("last_known_prev3_unit_price"),
        F.round(F.cume_dist().over(w_moved_at_month_by_moved_at_t), 3).alias("moved_at_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "operator",
        "unit_price",
        "qty_bucket",
        "rows_in_warehouse_id",
        "second_qty",
        "first_qty_bucket",
        "prev3_unit_price",
        "last_known_prev3_unit_price",
        "moved_at_cume_dist",
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
    )
)
