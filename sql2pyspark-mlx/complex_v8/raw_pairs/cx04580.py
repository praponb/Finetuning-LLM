from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_city = Window.partitionBy("warehouse_city")
w_move_type_by_moved_at_full = Window.partitionBy("move_type").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_operator_by_mov = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").asc_nulls_last(), "id")
w_all_by_unit_price_running = Window.orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_per_unit_price_ti = Window.orderBy(F.col("qty_per_unit_price").desc())
w_move_type_by_moved_at_runnin = Window.partitionBy("move_type").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_city_sku_id_by_qty = Window.partitionBy("warehouse_city", "sku_id").orderBy("qty_per_unit_price_dense_rank")
w_sku_id = Window.partitionBy("sku_id")
w_qty_per_unit_price_dense_ran = Window.partitionBy("qty_per_unit_price_dense_rank")
w_all_by_unit_price_vs_warehou = Window.orderBy(F.col("unit_price_vs_warehouse_id_avg").desc(), "id")
w_warehouse_city_sku_id_by_qty_2 = Window.partitionBy("warehouse_city", "sku_id").orderBy("qty_per_unit_price_dense_rank", "id")
w_warehouse_city_qty_per_unit = Window.partitionBy("warehouse_city", "qty_per_unit_price_dense_rank").orderBy(F.col("distinct_move_type_in_warehouse_city").desc(), "id")
w_all_by_unit_price_vs_warehou_2 = Window.orderBy(F.col("unit_price_vs_warehouse_id_avg").desc(), "id").rowsBetween(-1, Window.currentRow)
w_warehouse_city_sku_id_by_qty_3 = Window.partitionBy("warehouse_city", "sku_id").orderBy("qty_per_unit_price_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stock_moves
    .filter((F.col("sku_id") <= 3) & (F.col("warehouse_id") >= 4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

with_parent = (
    base.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.lower(F.col("w.wh_code")).like("%l1%")), "left")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.round(F.col("qty_per_unit_price") - F.avg("qty_per_unit_price").over(w_warehouse_city), 2).alias("qty_per_unit_price_vs_warehouse_city_avg"),
        F.last("warehouse_opened_on").over(w_move_type_by_moved_at_full).alias("last_warehouse_opened_on"),
        F.round(F.avg("warehouse_capacity").over(w_warehouse_id_operator_by_mov), 2).alias("warehouse_capacity_avg_7"),
        F.size(F.collect_set("warehouse_city").over(w_warehouse_id)).alias("distinct_warehouse_city_in_warehouse_id"),
        F.size(F.collect_set("move_type").over(w_warehouse_city)).alias("distinct_move_type_in_warehouse_city"),
        F.datediff(F.col("warehouse_opened_on"), F.lag("warehouse_opened_on").over(w_all_by_unit_price)).alias("days_since_prev_warehouse_opened_on"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id), 2).alias("unit_price_vs_warehouse_id_avg"),
        F.first("qty", ignorenulls=True).over(w_all_by_unit_price_running).alias("first_known_qty"),
        F.dense_rank().over(w_all_by_qty_per_unit_price_ti).alias("qty_per_unit_price_dense_rank"),
        F.count(F.lit(1)).over(w_warehouse_city).alias("rows_in_warehouse_city"),
        F.last("warehouse_opened_on", ignorenulls=True).over(w_move_type_by_moved_at_runnin).alias("last_known_warehouse_opened_on"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_warehouse_id"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "qty_per_unit_price",
        "warehouse_capacity",
        "warehouse_city",
        "qty_per_unit_price_vs_warehouse_city_avg",
        "last_warehouse_opened_on",
        "warehouse_capacity_avg_7",
        "distinct_warehouse_city_in_warehouse_id",
        "distinct_move_type_in_warehouse_city",
        "first_known_qty",
        "qty_per_unit_price_dense_rank",
        "rows_in_warehouse_city",
        "last_known_warehouse_opened_on",
        "unit_price_share_of_warehouse_id",
        F.round(F.percent_rank().over(w_warehouse_city_sku_id_by_qty), 3).alias("qty_per_unit_price_dense_rank_percent_rank"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.round(F.col("first_known_qty") - F.avg("first_known_qty").over(w_qty_per_unit_price_dense_ran), 2).alias("first_known_qty_vs_qty_per_unit_price_dense_rank"),
        F.ntile(3).over(w_all_by_unit_price_vs_warehou).alias("unit_price_vs_warehouse_id_avg_tercile"),
        F.lag("last_warehouse_opened_on", 3).over(w_warehouse_city_sku_id_by_qty_2).alias("prev3_last_warehouse_opened_on"),
        F.ntile(10).over(w_warehouse_city_qty_per_unit).alias("distinct_move_type_in_warehouse_city_decile"),
        F.round(F.avg("rows_in_warehouse_city").over(w_all_by_unit_price_vs_warehou_2), 2).alias("rows_in_warehouse_city_avg_2"),
        F.min("unit_price").over(w_warehouse_city_sku_id_by_qty_3).alias("min_unit_price_so_far"),
    )
)

result = (
    metrics
    .select(
        "id",
        "move_type",
        "unit_price",
        "qty_per_unit_price",
        "warehouse_capacity",
        "warehouse_city",
        "qty_per_unit_price_vs_warehouse_city_avg",
        "last_warehouse_opened_on",
        "warehouse_capacity_avg_7",
        "distinct_warehouse_city_in_warehouse_id",
        "distinct_move_type_in_warehouse_city",
        "qty_per_unit_price_dense_rank",
        "rows_in_warehouse_city",
        "last_known_warehouse_opened_on",
        "unit_price_share_of_warehouse_id",
        "rows_in_sku_id",
        "prev3_last_warehouse_opened_on",
        "distinct_move_type_in_warehouse_city_decile",
        "rows_in_warehouse_city_avg_2",
        "min_unit_price_so_far",
        F.length(F.trim(F.col("warehouse_city"))).alias("warehouse_city_len"),
    )
    .orderBy(
        F.col("distinct_warehouse_city_in_warehouse_id").asc(),
        F.col("id").asc_nulls_last(),
    )
)
