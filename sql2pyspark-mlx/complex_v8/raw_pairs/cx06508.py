from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_moved_at_month_warehouse_id = Window.partitionBy(F.date_trunc("month", "moved_at"), "warehouse_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price_running = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_sku_id_by_unit = Window.partitionBy("warehouse_id", "sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_prev3_moved_at_year_by_rows = Window.partitionBy(F.year("prev3_moved_at")).orderBy(F.col("rows_in_warehouse_id_operator").desc_nulls_first(), "id")
w_move_type_or_unknown_by_unit = Window.partitionBy("move_type_or_unknown").orderBy("unit_price_share_of_sku_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_or_unknown_by_unit_2 = Window.partitionBy("move_type_or_unknown").orderBy("unit_price_share_of_sku_id", "id")

prepared = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-01-31 07:00:00").cast("timestamp")) & (F.col("unit_price") < 293.2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_eve", "op_chan"), "group_a").when(F.col("operator") == "op_ann", "group_b").otherwise("other").alias("operator_group"),
    )
)

windowed = (
    prepared
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "operator_group",
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id), F.lit(0)), 4).alias("unit_price_share_of_sku_id"),
        F.count(F.lit(1)).over(w_warehouse_id_operator).alias("rows_in_warehouse_id_operator"),
        F.min("qty").over(w_sku_id).alias("sku_id_min_qty"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id), 2).alias("qty_vs_sku_id_avg"),
        F.max("unit_price").over(w_moved_at_month_warehouse_id).alias("max_unit_price_so_far"),
        F.last("moved_at", ignorenulls=True).over(w_all_by_unit_price_running).alias("last_known_moved_at"),
        F.nth_value("moved_at", 2).over(w_warehouse_id_sku_id_by_unit).alias("second_moved_at"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_operator), 2).alias("unit_price_vs_warehouse_id_operator_avg"),
        F.last("unit_price", ignorenulls=True).over(w_all_by_unit_price_running).alias("last_known_unit_price"),
        F.ntile(10).over(w_all_by_unit_price).alias("unit_price_decile"),
        F.lag("moved_at", 3).over(w_all_by_unit_price).alias("prev3_moved_at"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        (F.col("prev3_moved_at") + F.expr("INTERVAL 2 HOURS")).alias("prev3_moved_at_plus_2h"),
    )
)

windowed_2 = (
    annotated
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price",
        "operator_group",
        "unit_price_share_of_sku_id",
        "sku_id_min_qty",
        "last_known_moved_at",
        "second_moved_at",
        "unit_price_vs_warehouse_id_operator_avg",
        "last_known_unit_price",
        "prev3_moved_at",
        "move_type_or_unknown",
        F.row_number().over(w_prev3_moved_at_year_by_rows).alias("rn"),
        F.first("qty_vs_sku_id_avg").over(w_move_type_or_unknown_by_unit).alias("first_qty_vs_sku_id_avg"),
        F.row_number().over(w_move_type_or_unknown_by_unit_2).alias("unit_price_share_of_sku_id_seq"),
        (F.col("qty_vs_sku_id_avg") - F.lag("qty_vs_sku_id_avg").over(w_move_type_or_unknown_by_unit_2)).alias("qty_vs_sku_id_avg_change"),
    )
)

per_group = (
    windowed_2
    .groupBy(
        F.to_date("second_moved_at").alias("second_moved_at_day"),
        F.date_trunc("month", "prev3_moved_at").alias("prev3_moved_at_month"),
        "operator_group",
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.min("last_known_moved_at").alias("first_last_known_moved_at"),
        F.count("*").alias("row_count"),
        F.max("sku_id_min_qty").alias("max_sku_id_min_qty_check"),
    )
    .filter(F.col("max_sku_id_min_qty_check") > 1501)
    .drop("max_sku_id_min_qty_check")
)

result = (
    per_group
    .select(
        "second_moved_at_day",
        "avg_unit_price",
        "first_last_known_moved_at",
        F.abs(F.col("row_count") - 5).alias("row_count_distance"),
        (F.col("prev3_moved_at_month") + F.expr("INTERVAL 7 DAYS")).alias("prev3_moved_at_month_plus_7d"),
    )
    .orderBy(
        F.col("prev3_moved_at_month_plus_7d").desc(),
        F.col("avg_unit_price").desc(),
    )
)
