from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_minus_unit_price = Window.orderBy(F.col("qty_minus_unit_price").desc(), "id")
w_all_by_qty_minus_unit_price_2 = Window.orderBy(F.col("qty_minus_unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_all_by_qty_prior_2 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-2, -1)
w_sku_id_distinct_warehouse_id = Window.partitionBy("sku_id", "distinct_warehouse_id_in_sku_id_operator").orderBy("unit_price_vs_sku_id_operator_avg", "id")
w_rn = Window.partitionBy("rn")
w_sku_id_distinct_warehouse_id_2 = Window.partitionBy("sku_id", "distinct_warehouse_id_in_sku_id_operator").orderBy("unit_price_vs_sku_id_operator_avg", "id").rowsBetween(-2, Window.currentRow)

base_moves = (
    df_stock_moves
    .filter((F.col("move_type") == "ADJUST") & (F.col("warehouse_id") <= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

windowed = (
    base_moves
    .select(
        "*",
        F.row_number().over(w_all_by_qty_minus_unit_price).alias("rn"),
        F.first("qty_minus_unit_price").over(w_all_by_qty_minus_unit_price_2).alias("first_qty_minus_unit_price"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id_operator)).alias("distinct_warehouse_id_in_sku_id_operator"),
        F.lead("qty_minus_unit_price", 2, 0).over(w_warehouse_id_by_moved_at).alias("next2_qty_minus_unit_price"),
        F.round(F.avg("unit_price").over(w_sku_id_operator), 2).alias("sku_id_operator_avg_unit_price"),
        F.lag("unit_price").over(w_warehouse_id_by_moved_at).alias("prev_unit_price"),
        F.size(F.collect_set("move_type").over(w_sku_id_operator)).alias("distinct_move_type_in_sku_id_operator"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_operator), 2).alias("unit_price_vs_sku_id_operator_avg"),
        F.lag("qty_minus_unit_price", 3).over(w_warehouse_id_by_moved_at).alias("prev3_qty_minus_unit_price"),
    )
)

first_rows = windowed.filter(F.col("rn") <= 5)

windowed_2 = (
    first_rows
    .select(
        "*",
        F.sum("next2_qty_minus_unit_price").over(w_all_by_qty_prior_2).alias("next2_qty_minus_unit_price_sum_2_prior"),
        F.ntile(10).over(w_sku_id_distinct_warehouse_id).alias("unit_price_vs_sku_id_operator_avg_decile"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_rn), 2).alias("unit_price_vs_rn_avg"),
        F.size(F.collect_set("move_type").over(w_rn)).alias("distinct_move_type_in_rn"),
        F.round(F.avg("qty").over(w_sku_id_distinct_warehouse_id_2), 2).alias("qty_avg_3"),
        F.min("prev_unit_price").over(w_rn).alias("rn_min_prev_unit_price"),
    )
)

stats = (
    windowed_2
    .groupBy(
        "distinct_warehouse_id_in_sku_id_operator",
        "distinct_move_type_in_sku_id_operator",
    )
    .agg(
        F.round(F.sum("rn_min_prev_unit_price"), 2).alias("total_rn_min_prev_unit_price"),
        F.countDistinct("operator_or_unknown").alias("distinct_operator_or_unknowns"),
        F.max("unit_price_vs_sku_id_operator_avg_decile").alias("max_unit_price_vs_sku_id_operator_avg_decile"),
    )
)

result = (
    stats
    .select(
        "distinct_warehouse_id_in_sku_id_operator",
        "distinct_move_type_in_sku_id_operator",
        "total_rn_min_prev_unit_price",
        "distinct_operator_or_unknowns",
    )
    .orderBy(
        F.col("distinct_warehouse_id_in_sku_id_operator").asc_nulls_last(),
        F.col("total_rn_min_prev_unit_price").desc(),
        "distinct_move_type_in_sku_id_operator",
    )
    .limit(20)
)
