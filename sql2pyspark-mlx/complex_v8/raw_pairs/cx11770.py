from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_operator_by_qty_running = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_month_operator_by_m = Window.partitionBy(F.month("moved_at"), "operator").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_operator_len_by_warehouse_ca = Window.partitionBy("operator_len").orderBy(F.col("warehouse_capacity").desc(), "id")
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_warehouse_city_warehouse_id = Window.partitionBy("warehouse_city", "warehouse_id")
w_sku_id_by_warehouse_capacity = Window.partitionBy("sku_id").orderBy("warehouse_capacity")
w_sku_id_moved_at_month = Window.partitionBy("sku_id", F.date_trunc("month", "moved_at"))
w_moved_at_month_operator_by_m_2 = Window.partitionBy(F.month("moved_at"), "operator").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-3, -1)

valid_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) | (F.col("qty") >= 138))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

with_details_warehouse = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.operator_len",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

ranked = (
    with_details_warehouse
    .select(
        "*",
        F.min("operator_len").over(w_warehouse_id_sku_id).alias("warehouse_id_sku_id_min_operator_len"),
        F.first("warehouse_capacity", ignorenulls=True).over(w_operator_by_qty_running).alias("first_known_warehouse_capacity"),
        F.max("warehouse_capacity").over(w_warehouse_id_sku_id).alias("warehouse_id_sku_id_max_warehouse_capacity"),
        F.round(F.col("operator_len") - F.avg("operator_len").over(w_warehouse_id_sku_id), 2).alias("operator_len_vs_warehouse_id_sku_id_avg"),
        F.round(F.avg("warehouse_capacity").over(w_moved_at_month_operator_by_m), 2).alias("warehouse_capacity_avg_2"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_operator_len_by_warehouse_ca))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id_sku_id), 2).alias("qty_vs_warehouse_id_sku_id_avg"),
        F.lag("qty").over(w_all_by_qty).alias("prev_qty"),
        F.round(F.col("operator_len") - F.avg("operator_len").over(w_warehouse_city_warehouse_id), 2).alias("operator_len_vs_warehouse_city_warehouse_id_avg"),
        F.dense_rank().over(w_sku_id_by_warehouse_capacity).alias("warehouse_capacity_dense_rank"),
        F.count(F.lit(1)).over(w_sku_id_moved_at_month).alias("rows_in_sku_id_moved_at_month"),
        F.round(F.avg("unit_price").over(w_warehouse_city_warehouse_id), 2).alias("warehouse_city_warehouse_id_avg_unit_price"),
        F.round(F.avg("operator_len").over(w_moved_at_month_operator_by_m_2), 2).alias("operator_len_avg_3_prior"),
        (F.col("warehouse_capacity") - F.lag("warehouse_capacity").over(w_all_by_qty)).alias("warehouse_capacity_change"),
    )
)

top_rows = ranked.filter(F.col("warehouse_capacity_dense_rank") <= 3)

result = (
    top_rows
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "operator_len",
        "warehouse_city",
        "warehouse_capacity",
        "warehouse_wh_code",
        "warehouse_id_sku_id_min_operator_len",
        "warehouse_id_sku_id_max_warehouse_capacity",
        "operator_len_vs_warehouse_id_sku_id_avg",
        "minutes_since_prev_moved_at",
        "qty_vs_warehouse_id_sku_id_avg",
        "warehouse_capacity_dense_rank",
        "rows_in_sku_id_moved_at_month",
        "operator_len_avg_3_prior",
        "warehouse_capacity_change",
    )
    .orderBy("qty_vs_warehouse_id_sku_id_avg", F.col("operator_len").desc())
)
