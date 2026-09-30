from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_warehouse_opened_o = Window.partitionBy("sku_id").orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(-3, -1)
w_sku_id = Window.partitionBy("sku_id")
w_all_by_warehouse_opened_on = Window.orderBy("warehouse_opened_on", "id")
w_warehouse_opened_on_month_op = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on"), "operator")
w_all_by_warehouse_opened_on_r = Window.orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_moves = (
    df_stock_moves
    .filter(F.col("qty").between(91, 394))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.when(F.col("qty") >= 245, "HIGH").when(F.col("qty") >= 111, "MEDIUM").otherwise("LOW").alias("qty_tier"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

enriched = (
    cleaned_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.upper(F.trim(F.col("w.wh_code"))).like("B%"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.qty_tier",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.sum("qty").over(w_sku_id_by_warehouse_opened_o).alias("qty_sum_3_prior"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id), 2).alias("qty_vs_sku_id_avg"),
        F.round(F.cume_dist().over(w_all_by_warehouse_opened_on), 3).alias("warehouse_opened_on_cume_dist"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_opened_on_month_op), 2).alias("qty_vs_warehouse_opened_on_month_operator_avg"),
        F.min("qty").over(w_all_by_warehouse_opened_on_r).alias("min_qty_so_far"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.size(F.collect_set("operator").over(w_sku_id)).alias("distinct_operator_in_sku_id"),
        F.round(F.avg("qty").over(w_warehouse_opened_on_month_op), 2).alias("warehouse_opened_on_month_operator_avg_qty"),
    )
)

stats = (
    sequenced
    .groupBy(
        "operator",
        F.date_trunc("month", "warehouse_opened_on").alias("warehouse_opened_on_month"),
        "distinct_operator_in_sku_id",
    )
    .agg(
        F.round(F.stddev("warehouse_opened_on_cume_dist"), 2).alias("stddev_warehouse_opened_on_cume_dist"),
        F.datediff(F.max("warehouse_opened_on"), F.min("warehouse_opened_on")).alias("warehouse_opened_on_span_days"),
    )
)

result = (
    stats
    .filter(F.col("distinct_operator_in_sku_id") <= 7)
    .select(
        "operator",
        "distinct_operator_in_sku_id",
        "stddev_warehouse_opened_on_cume_dist",
        "warehouse_opened_on_span_days",
        F.when(F.col("operator").isin("op_chan", "op_ann"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group"),
    )
    .orderBy("operator_group")
)
