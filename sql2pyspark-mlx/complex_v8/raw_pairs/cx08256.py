from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_warehouse_id = Window.partitionBy(F.month("moved_at"), "warehouse_id")
w_operator_by_moved_at_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_moved_at_full = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_operator_by_unit_price_per_q = Window.partitionBy("operator").orderBy(F.col("unit_price_per_qty").asc_nulls_last(), "id")
w_all_by_unit_price_per_qty_ti = Window.orderBy("unit_price_per_qty")
w_last_known_moved_at_month_la = Window.partitionBy(F.month("last_known_moved_at"), "last_unit_price_per_qty_tier").orderBy(F.col("last_known_moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator = Window.partitionBy("operator")
w_last_known_moved_at_month_la_2 = Window.partitionBy(F.month("last_known_moved_at"), "last_unit_price_per_qty_tier").orderBy(F.col("last_known_moved_at").desc(), "id")
w_move_type_by_unit_price_per = Window.partitionBy("move_type").orderBy(F.col("unit_price_per_qty").desc(), "id")
w_move_type_by_unit_price_per_2 = Window.partitionBy("move_type").orderBy(F.col("unit_price_per_qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_pct_move_type_in_outb = Window.orderBy("pct_move_type_in_outbound_adjust", "first_known_moved_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_known_moved_at_month_m = Window.partitionBy(F.month("first_known_moved_at_month")).orderBy("median_operator_max_unit_price_per_qty_cume_dist")
w_first_known_moved_at_month_m_2 = Window.partitionBy(F.month("first_known_moved_at_month")).orderBy("median_operator_max_unit_price_per_qty_cume_dist", "first_known_moved_at_month")

recent = (
    df_stock_moves
    .filter(F.col("operator").isin("op_bee", "op_ann"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

with_parent_warehouse = (
    recent.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.wh_code").isNotNull())
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.operator_clean",
        "s.unit_price_per_qty",
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

sequenced = (
    with_parent_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price_per_qty",
        "warehouse_wh_code",
        F.size(F.collect_set("sku_id").over(w_moved_at_month_warehouse_id)).alias("distinct_sku_id_in_moved_at_month_warehouse_id"),
        F.last("moved_at", ignorenulls=True).over(w_operator_by_moved_at_running).alias("last_known_moved_at"),
        F.last("unit_price_per_qty").over(w_operator_by_moved_at_full).alias("last_unit_price_per_qty"),
        F.last("unit_price_per_qty", ignorenulls=True).over(w_operator_by_moved_at_running).alias("last_known_unit_price_per_qty"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id_move_type)).alias("distinct_sku_id_in_warehouse_id_move_type"),
        F.lead("unit_price_per_qty").over(w_operator_by_unit_price_per_q).alias("next_unit_price_per_qty"),
        F.first("moved_at", ignorenulls=True).over(w_operator_by_moved_at_running).alias("first_known_moved_at"),
        F.round(F.col("unit_price_per_qty") - F.avg("unit_price_per_qty").over(w_warehouse_id_move_type), 2).alias("unit_price_per_qty_vs_warehouse_id_move_type_avg"),
        F.round(F.cume_dist().over(w_all_by_unit_price_per_qty_ti), 3).alias("unit_price_per_qty_cume_dist"),
        F.round(F.col("unit_price_per_qty") / F.nullif(F.sum("unit_price_per_qty").over(w_moved_at_month_warehouse_id), F.lit(0)), 4).alias("unit_price_per_qty_share_of_moved_at_month_wareh"),
        F.size(F.collect_set("move_type").over(w_moved_at_month_warehouse_id)).alias("distinct_move_type_in_moved_at_month_warehouse_i"),
        F.min("unit_price_per_qty").over(w_warehouse_id_move_type).alias("warehouse_id_move_type_min_unit_price_per_qty"),
        F.round(F.avg("unit_price_per_qty").over(w_warehouse_id_move_type), 2).alias("warehouse_id_move_type_avg_unit_price_per_qty"),
    )
)

derived = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price_per_qty",
        "warehouse_wh_code",
        "last_known_moved_at",
        "last_known_unit_price_per_qty",
        "distinct_sku_id_in_warehouse_id_move_type",
        "unit_price_per_qty_cume_dist",
        "unit_price_per_qty_share_of_moved_at_month_wareh",
        "distinct_move_type_in_moved_at_month_warehouse_i",
        "warehouse_id_move_type_min_unit_price_per_qty",
        "warehouse_id_move_type_avg_unit_price_per_qty",
        F.when(F.col("last_unit_price_per_qty") >= 8.5, "A").when(F.col("last_unit_price_per_qty") >= 2.5, "B").otherwise("C").alias("last_unit_price_per_qty_tier"),
    )
)

metrics = (
    derived
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "last_known_moved_at",
        "last_known_unit_price_per_qty",
        "distinct_sku_id_in_warehouse_id_move_type",
        "warehouse_id_move_type_min_unit_price_per_qty",
        "last_unit_price_per_qty_tier",
        F.last("moved_at").over(w_last_known_moved_at_month_la).alias("last_moved_at"),
        F.max("unit_price_per_qty_cume_dist").over(w_operator).alias("operator_max_unit_price_per_qty_cume_dist"),
        F.round(F.col("unit_price_per_qty") - F.avg("unit_price_per_qty").over(w_operator), 2).alias("unit_price_per_qty_vs_operator_avg"),
        F.row_number().over(w_last_known_moved_at_month_la_2).alias("seq_no"),
        F.rank().over(w_move_type_by_unit_price_per).alias("unit_price_per_qty_rank"),
        F.first("moved_at", ignorenulls=True).over(w_move_type_by_unit_price_per_2).alias("first_known_moved_at"),
    )
)

per_group = (
    metrics
    .groupBy(
        F.date_trunc("month", "first_known_moved_at").alias("first_known_moved_at_month"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("move_type").isin("OUTBOUND", "ADJUST"), 1)) / F.count("*"), 1).alias("pct_move_type_in_outbound_adjust"),
        F.sum("seq_no").alias("total_seq_no"),
        F.percentile_approx("operator_max_unit_price_per_qty_cume_dist", 0.5).alias("median_operator_max_unit_price_per_qty_cume_dist"),
        F.min("warehouse_id_move_type_min_unit_price_per_qty").alias("min_warehouse_id_move_type_min_unit_price_per_qt"),
        F.sum("unit_price_per_qty_vs_operator_avg").alias("total_unit_price_per_qty_vs_operator_avg"),
    )
)

windowed = (
    per_group
    .select(
        "first_known_moved_at_month",
        "pct_move_type_in_outbound_adjust",
        "total_seq_no",
        "median_operator_max_unit_price_per_qty_cume_dist",
        F.nth_value("total_seq_no", 2).over(w_all_by_pct_move_type_in_outb).alias("second_total_seq_no"),
        F.round(F.percent_rank().over(w_first_known_moved_at_month_m), 3).alias("median_operator_max_unit_price_per_qty_cume_dist_2"),
        F.row_number().over(w_first_known_moved_at_month_m_2).alias("seq_no"),
    )
)

result = (
    windowed
    .select(
        "first_known_moved_at_month",
        "pct_move_type_in_outbound_adjust",
        "median_operator_max_unit_price_per_qty_cume_dist",
        "second_total_seq_no",
        "seq_no",
        F.coalesce(F.col("second_total_seq_no"), F.lit(0)).alias("second_total_seq_no_filled"),
        F.least(F.col("second_total_seq_no"), F.col("seq_no")).alias("least_second_total_seq_no_seq_no"),
    )
    .orderBy(
        F.col("pct_move_type_in_outbound_adjust").asc_nulls_last(),
        F.col("second_total_seq_no_filled").asc_nulls_last(),
    )
)
