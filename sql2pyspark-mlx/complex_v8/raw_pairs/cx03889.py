from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_moved_at_month_by = Window.partitionBy("move_type", F.month("moved_at")).orderBy(F.col("unit_price").asc_nulls_last())
w_moved_at_year = Window.partitionBy(F.year("moved_at"))
w_move_type_moved_at_month_by_2 = Window.partitionBy("move_type", F.month("moved_at")).orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())
w_sku_id_moved_at_year_by_qty = Window.partitionBy("sku_id", F.year("moved_at")).orderBy(F.col("qty").desc(), "id")
w_qty_rank_sku_id_by_qty_full = Window.partitionBy("qty_rank", "sku_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_qty_decile_by_unit_price_den = Window.partitionBy("qty_decile").orderBy("unit_price_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type = Window.partitionBy("move_type")

prepared = (
    df_stock_moves
    .filter(F.col("move_type").isin("OUTBOUND", "TRANSFER", "ADJUST"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "unit_price_minus_qty",
        "moved_at_label",
        F.dense_rank().over(w_move_type_moved_at_month_by).alias("unit_price_dense_rank"),
        F.size(F.collect_set("move_type").over(w_moved_at_year)).alias("distinct_move_type_in_moved_at_year"),
        F.sum("qty").over(w_moved_at_year).alias("moved_at_year_sum_qty"),
        F.round(F.avg("qty").over(w_moved_at_year), 2).alias("moved_at_year_avg_qty"),
        F.sum("unit_price_minus_qty").over(w_move_type_moved_at_month_by_2).alias("running_unit_price_minus_qty"),
        F.rank().over(w_all_by_qty_ties).alias("qty_rank"),
        F.count(F.lit(1)).over(w_moved_at_year).alias("rows_in_moved_at_year"),
        F.dense_rank().over(w_sku_id_moved_at_year_by_qty).alias("qty_dense_rank"),
        F.ntile(10).over(w_sku_id_moved_at_year_by_qty).alias("qty_decile"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.last("qty").over(w_qty_rank_sku_id_by_qty_full).alias("last_qty"),
        F.sum("unit_price").over(w_qty_decile_by_unit_price_den).alias("running_unit_price"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.round(F.avg("moved_at_year_avg_qty").over(w_move_type), 2).alias("move_type_avg_moved_at_year_avg_qty"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "move_type",
        "qty",
        "unit_price",
        "unit_price_minus_qty",
        "moved_at_label",
        "unit_price_dense_rank",
        "moved_at_year_sum_qty",
        "moved_at_year_avg_qty",
        "running_unit_price_minus_qty",
        "rows_in_moved_at_year",
        "qty_dense_rank",
        "qty_decile",
        "last_qty",
        "running_unit_price",
        "rows_in_move_type",
        F.least(F.col("unit_price_dense_rank"), F.col("moved_at_year_sum_qty")).alias("least_unit_price_dense_rank_moved_at_year_sum_qt"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
    .orderBy("moved_at_year_avg_qty")
)
