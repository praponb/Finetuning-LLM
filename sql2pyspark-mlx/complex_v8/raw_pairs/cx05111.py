from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_running = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_unit_price_plus_qt = Window.partitionBy("sku_id").orderBy("unit_price_plus_qty", "id")
w_sku_id_by_unit_price_plus_qt_2 = Window.partitionBy("sku_id").orderBy("unit_price_plus_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_full = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_unit_price_moving_2 = Window.orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w_all_by_moved_at_plus_7d_prio = Window.orderBy("moved_at_plus_7d", "id").rowsBetween(-2, -1)
w_all_by_next2_qty = Window.orderBy(F.col("next2_qty").desc(), "id")
w_all_by_moved_at_plus_7d = Window.orderBy("moved_at_plus_7d", "id")
w_row_count_by_row_count = Window.partitionBy("row_count").orderBy(F.col("row_count").desc(), "moved_at_plus_7d_day", "warehouse_id", "unit_price_plus_qty_seq")
w_all_by_row_count = Window.orderBy("row_count", "moved_at_plus_7d_day", "warehouse_id", "unit_price_plus_qty_seq")
w_unit_price_plus_qty_seq = Window.partitionBy("unit_price_plus_qty_seq")

prepared = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.last("qty", ignorenulls=True).over(w_all_by_qty_running).alias("last_known_qty"),
        F.row_number().over(w_sku_id_by_unit_price_plus_qt).alias("unit_price_plus_qty_seq"),
        F.last("qty", ignorenulls=True).over(w_sku_id_by_unit_price_plus_qt_2).alias("last_known_qty_2"),
        F.nth_value("unit_price_plus_qty", 2).over(w_all_by_qty_full).alias("second_unit_price_plus_qty"),
        F.lead("qty", 2).over(w_all_by_unit_price).alias("next2_qty"),
        F.round(F.col("unit_price_plus_qty") - F.avg("unit_price_plus_qty").over(w_warehouse_id), 2).alias("unit_price_plus_qty_vs_warehouse_id_avg"),
        F.last("unit_price", ignorenulls=True).over(w_all_by_qty_running).alias("last_known_unit_price"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_unit_price)).alias("unit_price_change"),
        F.max("qty").over(w_warehouse_id).alias("warehouse_id_max_qty"),
        F.round(F.avg("qty").over(w_all_by_unit_price_moving_2), 2).alias("qty_avg_3"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id), 2).alias("unit_price_vs_warehouse_id_avg"),
    )
)

top_rows = with_trends.filter(F.col("unit_price_plus_qty_seq") == 1)

with_windows = (
    top_rows
    .select(
        "*",
        F.max("last_known_unit_price").over(w_all_by_moved_at_plus_7d_prio).alias("last_known_unit_price_max_2_prior"),
        F.lag("last_known_unit_price").over(w_all_by_next2_qty).alias("prev_last_known_unit_price"),
        F.row_number().over(w_all_by_moved_at_plus_7d).alias("seq_no"),
    )
)

stats = (
    with_windows
    .groupBy(
        F.to_date("moved_at_plus_7d").alias("moved_at_plus_7d_day"),
        "warehouse_id",
        "unit_price_plus_qty_seq",
    )
    .agg(
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 2)
)

sequenced = (
    stats
    .select(
        "*",
        F.ntile(10).over(w_row_count_by_row_count).alias("row_count_decile"),
        F.row_number().over(w_all_by_row_count).alias("seq_no"),
        F.size(F.collect_set("warehouse_id").over(w_unit_price_plus_qty_seq)).alias("distinct_warehouse_id_in_unit_price_plus_qty_seq"),
    )
)

result = (
    sequenced
    .select(
        "moved_at_plus_7d_day",
        "warehouse_id",
        "distinct_warehouse_ids",
        "row_count_decile",
        "seq_no",
        "distinct_warehouse_id_in_unit_price_plus_qty_seq",
        F.trunc(F.col("moved_at_plus_7d_day"), "MM").alias("moved_at_plus_7d_day_month_start"),
    )
    .orderBy("row_count_decile")
)
