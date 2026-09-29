from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_operators_cnt = Window.partitionBy("distinct_operators", "cnt")
w_distinct_sku_ids_cnt_by_qty = Window.partitionBy("distinct_sku_ids", "cnt").orderBy("qty_move_type_in_adjust_transfer", "warehouse_id").rowsBetween(-3, -1)
w_all_by_distinct_operators = Window.orderBy(F.col("distinct_operators").desc(), "warehouse_id")

filtered_moves = (
    df_stock_moves
    .filter((F.col("sku_id") <= 13) & (F.col("qty") <= 256))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
    )
)

totals = (
    filtered_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("qty").alias("max_qty"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.count("*").alias("cnt"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.min("unit_price").alias("min_unit_price"),
        F.sum(F.when(F.col("move_type").isin("ADJUST", "TRANSFER", "OUTBOUND"), F.col("qty"))).alias("qty_move_type_in_adjust_transfer"),
    )
    .filter(F.col("cnt") >= 2)
)

enriched_warehouse = (
    totals.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.warehouse_id",
        "s.distinct_sku_ids",
        "s.cnt",
        "s.distinct_operators",
        "s.min_unit_price",
        "s.qty_move_type_in_adjust_transfer",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

with_details = (
    enriched_warehouse.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code_2"),
        F.col("w.capacity").alias("warehouse_capacity_2"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.round(F.col("min_unit_price") - F.avg("min_unit_price").over(w_distinct_operators_cnt), 2).alias("min_unit_price_vs_distinct_operators_cnt_avg"),
        F.sum("min_unit_price").over(w_distinct_sku_ids_cnt_by_qty).alias("min_unit_price_sum_3_prior"),
        F.ntile(3).over(w_all_by_distinct_operators).alias("distinct_operators_tercile"),
        F.count(F.lit(1)).over(w_distinct_operators_cnt).alias("rows_in_distinct_operators_cnt"),
    )
)

result = (
    with_windows
    .select(
        "warehouse_id",
        "cnt",
        "distinct_operators",
        "min_unit_price",
        "qty_move_type_in_adjust_transfer",
        "warehouse_capacity",
        "warehouse_wh_code",
        "warehouse_opened_on",
        "warehouse_capacity_2",
        "min_unit_price_vs_distinct_operators_cnt_avg",
        "min_unit_price_sum_3_prior",
        "distinct_operators_tercile",
        "rows_in_distinct_operators_cnt",
    )
    .orderBy(
        F.col("min_unit_price_vs_distinct_operators_cnt_avg").asc_nulls_last(),
    )
)
