from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month = Window.partitionBy(F.date_trunc("month", "moved_at"))
w_all_by_qty_bucket_running = Window.orderBy(F.col("qty_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_bucket_ties = Window.orderBy(F.col("qty_bucket").desc())
w_warehouse_id_by_unit_price_r = Window.partitionBy("warehouse_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_bucket = Window.orderBy(F.col("qty_bucket").desc(), "id")
w_warehouse_id_by_qty_bucket = Window.partitionBy("warehouse_id").orderBy("qty_bucket", "id")
w_warehouse_id_by_qty_bucket_t = Window.partitionBy("warehouse_id").orderBy("qty_bucket")
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy("unit_price", "id")
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_all_by_moved_at_month_sum_qt = Window.orderBy(F.col("moved_at_month_sum_qty").desc(), "id").rowsBetween(-6, Window.currentRow)

filtered_moves = (
    df_stock_moves
    .filter((F.col("unit_price") < 393.3) & (F.col("qty") < 387))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

ranked = (
    filtered_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_moved_at_month), F.lit(0)), 4).alias("unit_price_share_of_moved_at_month"),
        F.first("qty", ignorenulls=True).over(w_all_by_qty_bucket_running).alias("first_known_qty"),
        F.min("qty_bucket").over(w_moved_at_month).alias("moved_at_month_min_qty_bucket"),
        F.round(F.percent_rank().over(w_all_by_qty_bucket_ties), 3).alias("qty_bucket_percent_rank"),
        F.sum("qty").over(w_moved_at_month).alias("moved_at_month_sum_qty"),
        F.count(F.lit(1)).over(w_warehouse_id_by_unit_price_r).alias("running_count"),
        F.lag("moved_at", 3).over(w_all_by_qty_bucket).alias("prev3_moved_at"),
        F.sum("unit_price").over(w_moved_at_month).alias("moved_at_month_sum_unit_price"),
        F.lead("moved_at").over(w_warehouse_id_by_qty_bucket).alias("next_moved_at"),
        F.rank().over(w_warehouse_id_by_qty_bucket_t).alias("qty_bucket_rank"),
        F.count(F.lit(1)).over(w_moved_at_month).alias("rows_in_moved_at_month"),
        F.row_number().over(w_warehouse_id_by_unit_price).alias("unit_price_seq"),
        F.lag("qty", 2).over(w_warehouse_id_by_unit_price).alias("prev2_qty"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.count(F.lit(1)).over(w_sku_id_warehouse_id).alias("rows_in_sku_id_warehouse_id"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_id_warehouse_id), F.lit(0)), 4).alias("qty_share_of_sku_id_warehouse_id"),
        F.round(F.avg("unit_price_seq").over(w_all_by_moved_at_month_sum_qt), 2).alias("unit_price_seq_avg_7"),
        F.min("prev2_qty").over(w_sku_id_warehouse_id).alias("sku_id_warehouse_id_min_prev2_qty"),
    )
)

result = (
    with_trends
    .filter(F.col("moved_at_month_sum_unit_price") < 4500.1)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "unit_price_share_of_moved_at_month",
        "moved_at_month_min_qty_bucket",
        "qty_bucket_percent_rank",
        "running_count",
        "prev3_moved_at",
        "moved_at_month_sum_unit_price",
        "next_moved_at",
        "qty_bucket_rank",
        "rows_in_sku_id_warehouse_id",
        "qty_share_of_sku_id_warehouse_id",
        "unit_price_seq_avg_7",
        "sku_id_warehouse_id_min_prev2_qty",
        F.when(F.col("unit_price_seq") >= 8, "HIGH").when(F.col("unit_price_seq") >= 5, "MEDIUM").otherwise("LOW").alias("unit_price_seq_tier"),
    )
    .orderBy(
        F.col("moved_at_month_min_qty_bucket").desc(),
        F.col("unit_price_seq_avg_7").desc(),
    )
)
