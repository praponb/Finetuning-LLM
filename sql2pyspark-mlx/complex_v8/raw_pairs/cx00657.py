from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc(), "id")
w_moved_at_month_warehouse_ope = Window.partitionBy(F.date_trunc("month", "moved_at"), F.month("warehouse_opened_on")).orderBy(F.col("sku_unit_cost").desc(), "id")
w_moved_at_month_warehouse_ope_2 = Window.partitionBy(F.date_trunc("month", "moved_at"), F.month("warehouse_opened_on")).orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(-3, -1)
w_operator_by_warehouse_opened = Window.partitionBy("operator").orderBy("warehouse_opened_on", "id")
w_operator_by_warehouse_opened_2 = Window.partitionBy("operator").orderBy("warehouse_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month_warehouse_ope_3 = Window.partitionBy(F.date_trunc("month", "moved_at"), F.month("warehouse_opened_on")).orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(-1, -1)
w_sku_id_by_sku_id_warehouse_i = Window.partitionBy("sku_id").orderBy(F.col("sku_id_warehouse_id_sum_unit_price").desc())
w_warehouse_id_by_last_qty = Window.partitionBy("warehouse_id").orderBy("last_qty", "id")
w_operator = Window.partitionBy("operator")
w_sku_id_by_sku_id_warehouse_i_2 = Window.partitionBy("sku_id").orderBy(F.col("sku_id_warehouse_id_sum_unit_price").desc(), "id")

filtered_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.concat(F.col("operator"), F.lit("#"), F.col("qty").cast("string")).alias("operator_ref"),
    )
)

with_details = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("sk.unit_cost").between(17.3, 330.6))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_operator_label",
        "s.operator_ref",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.sum("unit_price").over(w_sku_id_warehouse_id).alias("sku_id_warehouse_id_sum_unit_price"),
        F.ntile(10).over(w_warehouse_id_sku_id_by_moved).alias("moved_at_decile"),
        F.count(F.lit(1)).over(w_sku_id_warehouse_id).alias("rows_in_sku_id_warehouse_id"),
        F.lead("sku_unit_cost", 1, 0).over(w_moved_at_month_warehouse_ope).alias("next_sku_unit_cost"),
        F.sum("sku_unit_cost").over(w_moved_at_month_warehouse_ope_2).alias("sku_unit_cost_sum_3_prior"),
        F.lead("moved_at").over(w_operator_by_warehouse_opened).alias("next_moved_at"),
        F.last("qty").over(w_operator_by_warehouse_opened_2).alias("last_qty"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id_warehouse_id), 2).alias("qty_vs_sku_id_warehouse_id_avg"),
        F.lead("unit_price").over(w_moved_at_month_warehouse_ope).alias("next_unit_price"),
        F.round(F.avg("qty").over(w_moved_at_month_warehouse_ope_3), 2).alias("qty_avg_1_prior"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "sku_id",
        "moved_at",
        "unit_price",
        "operator_ref",
        "sku_unit_cost",
        "warehouse_wh_code",
        "warehouse_opened_on",
        "sku_id_warehouse_id_sum_unit_price",
        "moved_at_decile",
        "rows_in_sku_id_warehouse_id",
        "sku_unit_cost_sum_3_prior",
        "next_moved_at",
        "last_qty",
        "qty_vs_sku_id_warehouse_id_avg",
        "next_unit_price",
        F.round(F.percent_rank().over(w_sku_id_by_sku_id_warehouse_i), 3).alias("sku_id_warehouse_id_sum_unit_price_percent_rank"),
        F.row_number().over(w_warehouse_id_by_last_qty).alias("last_qty_seq"),
        F.min("unit_price").over(w_operator).alias("operator_min_unit_price"),
        F.max("sku_unit_cost").over(w_operator).alias("operator_max_sku_unit_cost"),
        F.ntile(5).over(w_sku_id_by_sku_id_warehouse_i_2).alias("sku_id_warehouse_id_sum_unit_price_quintile"),
        F.round(F.col("next_unit_price") - F.avg("next_unit_price").over(w_operator), 2).alias("next_unit_price_vs_operator_avg"),
        F.max("qty_avg_1_prior").over(w_operator).alias("operator_max_qty_avg_1_prior"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
    )
)

grouped = (
    windowed
    .groupBy(F.date_trunc("month", "moved_at").alias("moved_at_month"))
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.round(100 * F.count(F.when(F.length(F.trim(F.col("warehouse_wh_code"))) >= 5, 1)) / F.count("*"), 1).alias("pct_length_trim_warehouse_wh_cod"),
    )
)

result = (
    grouped
    .filter(F.col("moved_at_month") < F.lit("2025-02-09 09:00:00").cast("timestamp"))
    .select(
        "moved_at_month",
        "pct_length_trim_warehouse_wh_cod",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at_month")).alias("days_since_moved_at_month"),
    )
    .orderBy(F.col("days_since_moved_at_month").asc())
)
