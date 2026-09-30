from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_running = Window.orderBy(F.col("moved_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_warehouse_id_moved_at_month = Window.partitionBy("warehouse_id", "moved_at_month")
w_sku_id_distinct_sku_id_in_mo = Window.partitionBy("sku_id", "distinct_sku_id_in_move_type_warehouse_id").orderBy(F.col("move_type_warehouse_id_min_moved_at_month").desc(), "id")
w_distinct_sku_id_in_move_type = Window.partitionBy("distinct_sku_id_in_move_type_warehouse_id")
w_warehouse_id_move_type_by_un = Window.partitionBy("warehouse_id", "move_type").orderBy("unit_price_vs_warehouse_id_moved_at_month_avg", "id").rowsBetween(-6, Window.currentRow)
w_move_type_or_unknown_by_dist = Window.partitionBy("move_type_or_unknown").orderBy(F.col("distinct_sku_id_in_move_type_warehouse_id").desc(), "id")
w_warehouse_id_move_type_by_un_2 = Window.partitionBy("warehouse_id", "move_type").orderBy("unit_price_vs_warehouse_id_moved_at_month_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_or_unknown_by_dist_2 = Window.partitionBy("move_type_or_unknown").orderBy(F.col("distinct_sku_id_in_move_type_warehouse_id").desc())

filtered = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.month(F.col("moved_at")).alias("moved_at_month"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.sum("unit_price").over(w_all_by_moved_at_running).alias("running_unit_price"),
        F.min("moved_at_month").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_min_moved_at_month"),
        F.round(F.col("moved_at_month") - F.avg("moved_at_month").over(w_move_type_warehouse_id), 2).alias("moved_at_month_vs_move_type_warehouse_id_avg"),
        F.size(F.collect_set("sku_id").over(w_move_type_warehouse_id)).alias("distinct_sku_id_in_move_type_warehouse_id"),
        F.dense_rank().over(w_all_by_moved_at).alias("moved_at_dense_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_moved_at_month), 2).alias("unit_price_vs_warehouse_id_moved_at_month_avg"),
        F.round(F.cume_dist().over(w_all_by_moved_at), 3).alias("moved_at_cume_dist"),
        F.count(F.lit(1)).over(w_warehouse_id_moved_at_month).alias("rows_in_warehouse_id_moved_at_month"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "moved_at_month",
        "move_type_or_unknown",
        "running_unit_price",
        "distinct_sku_id_in_move_type_warehouse_id",
        "moved_at_dense_rank",
        "moved_at_cume_dist",
        "rows_in_warehouse_id_moved_at_month",
        F.row_number().over(w_sku_id_distinct_sku_id_in_mo).alias("rn"),
        F.max("unit_price").over(w_distinct_sku_id_in_move_type).alias("distinct_sku_id_in_move_type_warehouse_id_max_un"),
        F.round(F.avg("moved_at_cume_dist").over(w_warehouse_id_move_type_by_un), 2).alias("moved_at_cume_dist_avg_7"),
        F.ntile(3).over(w_move_type_or_unknown_by_dist).alias("distinct_sku_id_in_move_type_warehouse_id_tercil"),
        F.last("moved_at_cume_dist", ignorenulls=True).over(w_warehouse_id_move_type_by_un_2).alias("last_known_moved_at_cume_dist"),
        F.size(F.collect_set("move_type_or_unknown").over(w_distinct_sku_id_in_move_type)).alias("distinct_move_type_or_unknown_in_distinct_sku_id"),
        F.round(F.avg("running_unit_price").over(w_distinct_sku_id_in_move_type), 2).alias("distinct_sku_id_in_move_type_warehouse_id_avg_ru"),
        F.round(F.percent_rank().over(w_move_type_or_unknown_by_dist_2), 3).alias("distinct_sku_id_in_move_type_warehouse_id_percen"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        "moved_at_month",
        "move_type_or_unknown",
        "running_unit_price",
        "moved_at_dense_rank",
        "rn",
        "distinct_sku_id_in_move_type_warehouse_id_max_un",
        "moved_at_cume_dist_avg_7",
        "last_known_moved_at_cume_dist",
        "distinct_sku_id_in_move_type_warehouse_id_percen",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
    .orderBy(F.col("moved_at_dense_rank").asc(), "id")
    .limit(10)
)
