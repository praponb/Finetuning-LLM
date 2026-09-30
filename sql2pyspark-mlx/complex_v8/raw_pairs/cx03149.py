from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_by_moved_at = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy("moved_at", "id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_all_by_rn_ties = Window.orderBy(F.col("rn").desc())
w_operator_group_by_rn_full = Window.partitionBy("operator_group").orderBy(F.col("rn").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn = Window.partitionBy("rn")
w_all_by_rn_moving_3 = Window.orderBy(F.col("rn").desc(), "id").rowsBetween(-3, Window.currentRow)

scoped_moves = (
    df_stock_moves
    .filter((F.col("operator").isin("op_chan", "op_ann", "op_dew")) & (F.col("warehouse_id") <= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_chan", "op_dew"), "group_a").when(F.col("operator") == "op_ann", "group_b").otherwise("other").alias("operator_group"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

ranked = (
    scoped_moves
    .select(
        "id",
        "qty",
        "operator",
        "unit_price",
        "operator_group",
        "unit_price_filled",
        F.lag("unit_price").over(w_moved_at_month_by_moved_at).alias("prev_unit_price"),
        F.row_number().over(w_moved_at_month_by_moved_at).alias("rn"),
        F.round(F.col("unit_price_filled") / F.nullif(F.sum("unit_price_filled").over(w_sku_id_operator), F.lit(0)), 4).alias("unit_price_filled_share_of_sku_id_operator"),
        F.round(F.cume_dist().over(w_moved_at_month_by_moved_at), 3).alias("moved_at_cume_dist"),
        F.max("qty").over(w_sku_id_operator).alias("sku_id_operator_max_qty"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id_operator)).alias("distinct_warehouse_id_in_sku_id_operator"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.dense_rank().over(w_all_by_rn_ties).alias("rn_dense_rank"),
        F.nth_value("rn", 2).over(w_operator_group_by_rn_full).alias("second_rn"),
        F.round(F.avg("rn").over(w_rn), 2).alias("rn_avg_rn"),
        F.round(F.avg("unit_price_filled").over(w_all_by_rn_moving_3), 2).alias("unit_price_filled_avg_4"),
    )
)

tagged = (
    windowed
    .select(
        "*",
        F.coalesce(F.col("second_rn"), F.lit(0)).alias("second_rn_filled"),
        F.when(F.col("operator").isin("op_chan", "op_bee"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group_2"),
    )
)

operator_group_stats = (
    tagged
    .groupBy("operator_group")
    .agg(F.percentile_approx("unit_price_filled_share_of_sku_id_operator", 0.5).alias("median_unit_price_filled_share_of_sku_id_operato"))
)

compared = (
    tagged.alias("s")
    .join(operator_group_stats.alias("g"), F.col("g.operator_group") == F.col("s.operator_group"), "inner")
    .filter(F.col("s.unit_price_filled_share_of_sku_id_operator") >= F.col("g.median_unit_price_filled_share_of_sku_id_operato"))
    .select(
        "s.*",
        "g.median_unit_price_filled_share_of_sku_id_operato",
        F.round(F.col("s.unit_price_filled_share_of_sku_id_operator") - F.col("g.median_unit_price_filled_share_of_sku_id_operato"), 2).alias("unit_price_filled_share_of_sku_id_operator_minus"),
    )
)

totals = (
    compared
    .filter(F.col("rn_avg_rn") >= 3.2)
    .groupBy("distinct_warehouse_id_in_sku_id_operator")
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.count("*").alias("n_rows"),
        F.round(F.stddev("sku_id_operator_max_qty"), 2).alias("stddev_sku_id_operator_max_qty"),
        F.min("prev_unit_price").alias("min_prev_unit_price"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    totals
    .select(
        "distinct_warehouse_id_in_sku_id_operator",
        "stddev_sku_id_operator_max_qty",
        "min_prev_unit_price",
    )
    .unpivot(
        ids=["distinct_warehouse_id_in_sku_id_operator"],
        values=["stddev_sku_id_operator_max_qty", "min_prev_unit_price"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("distinct_warehouse_id_in_sku_id_operator", "field")
)
