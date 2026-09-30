from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_moved_at_to_date_b = Window.partitionBy("move_type", F.to_date("moved_at")).orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_moved_at_month = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id = Window.partitionBy("sku_id")
w_moved_at_month_sku_id = Window.partitionBy(F.date_trunc("month", "moved_at"), "sku_id")
w_move_type_moved_at_to_date_b_2 = Window.partitionBy("move_type", F.to_date("moved_at")).orderBy("moved_at", "id")
w_moved_at_month_warehouse_id = Window.partitionBy(F.date_trunc("month", "moved_at"), "warehouse_id").orderBy("qty_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_moved_at_month_2 = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy(F.col("unit_price").desc(), "id")

valid = (
    df_stock_moves
    .filter(F.col("qty") >= 168)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket_2"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.count(F.lit(1)).over(w_move_type_moved_at_to_date_b).alias("running_count"),
        F.nth_value("qty_bucket", 2).over(w_warehouse_id_moved_at_month).alias("second_qty_bucket"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.size(F.collect_set("move_type").over(w_moved_at_month_sku_id)).alias("distinct_move_type_in_moved_at_month_sku_id"),
        F.round(F.cume_dist().over(w_move_type_moved_at_to_date_b_2), 3).alias("moved_at_cume_dist"),
        F.first("moved_at").over(w_moved_at_month_warehouse_id).alias("first_moved_at"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
        F.round(F.avg("qty_bucket").over(w_moved_at_month_sku_id), 2).alias("moved_at_month_sku_id_avg_qty_bucket"),
        F.lag("qty").over(w_warehouse_id_moved_at_month_2).alias("prev_qty"),
        F.min("qty_bucket").over(w_sku_id).alias("sku_id_min_qty_bucket"),
    )
)

result = (
    sequenced
    .filter(F.col("second_qty_bucket") <= 325)
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "second_qty_bucket",
        "distinct_move_type_in_moved_at_month_sku_id",
        "first_moved_at",
        "distinct_warehouse_id_in_sku_id",
        "moved_at_month_sku_id_avg_qty_bucket",
        "prev_qty",
        "sku_id_min_qty_bucket",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
    )
    .orderBy(F.col("moved_at").desc_nulls_first(), "id")
    .limit(10)
)
