from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_is_active_b = Window.partitionBy("warehouse_id", "sku_is_active").orderBy("qty", "id").rowsBetween(-3, -1)
w_operator_by_qty = Window.partitionBy("operator").orderBy("qty", "id")
w_operator_qty_rank_by_qty = Window.partitionBy("operator", "qty_rank").orderBy(F.col("qty").desc(), "id")
w_sku_is_active_operator = Window.partitionBy("sku_is_active", "operator")
w_operator_qty_rank_by_qty_tie = Window.partitionBy("operator", "qty_rank").orderBy(F.col("qty").desc())

filtered_moves = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
    )
)

with_details = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.move_type_clean",
        "s.moved_at_plus_30m",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

scored = (
    with_details
    .filter(F.col("sku_id") <= 9)
    .select(
        "id",
        "warehouse_id",
        "qty",
        "operator",
        "move_type_clean",
        "moved_at_plus_30m",
        "sku_is_active",
        F.round(F.col("sku_unit_cost") * 1.5, 2).alias("sku_unit_cost_adj"),
        F.concat(F.col("operator"), F.lit("#"), F.col("qty").cast("string")).alias("operator_ref"),
    )
)

with_trends = (
    scored
    .select(
        "id",
        "qty",
        "operator",
        "moved_at_plus_30m",
        "sku_is_active",
        "operator_ref",
        F.round(F.avg("qty").over(w_warehouse_id_sku_is_active_b), 2).alias("qty_avg_3_prior"),
        F.rank().over(w_operator_by_qty).alias("qty_rank"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "qty",
        "operator",
        "moved_at_plus_30m",
        "qty_rank",
        F.lag("qty_avg_3_prior").over(w_operator_qty_rank_by_qty).alias("prev_qty_avg_3_prior"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_is_active_operator), 2).alias("qty_vs_sku_is_active_operator_avg"),
        F.round(F.percent_rank().over(w_operator_qty_rank_by_qty_tie), 3).alias("qty_percent_rank"),
        F.round(F.col("qty_avg_3_prior") / F.nullif(F.sum("qty_avg_3_prior").over(w_sku_is_active_operator), F.lit(0)), 4).alias("qty_avg_3_prior_share_of_sku_is_active_operator"),
    )
)

totals = (
    sequenced
    .groupBy(
        "qty_rank",
        F.when(F.col("qty_percent_rank") >= 0.8, "A").when(F.col("qty_percent_rank") >= 0.2, "B").otherwise("C").alias("qty_percent_rank_tier"),
    )
    .agg(
        F.min("moved_at_plus_30m").alias("first_moved_at_plus_30m"),
        F.sum("qty_rank").alias("total_qty_rank"),
        F.count(F.when(F.col("operator").isin("op_bee", "op_eve"), 1)).alias("n_operator_in_op_bee_op_eve"),
        F.min("qty_percent_rank").alias("min_qty_percent_rank"),
        F.max("qty").alias("max_qty"),
        F.round(F.sum("qty_percent_rank") / F.nullif(F.sum("prev_qty_avg_3_prior"), F.lit(0)), 3).alias("qty_percent_rank_to_prev_qty_avg_3_prior_ratio"),
        F.sum("qty_percent_rank").alias("sum_qty_percent_rank_check"),
    )
    .filter(F.col("sum_qty_percent_rank_check") > 0.3)
    .drop("sum_qty_percent_rank_check")
)

qualified = (
    totals
    .crossJoin(totals.agg(F.max("min_qty_percent_rank").alias("overall_max_min_qty_percent_rank")))
    .filter(F.col("min_qty_percent_rank") < F.col("overall_max_min_qty_percent_rank"))
    .drop("overall_max_min_qty_percent_rank")
)

result = (
    qualified
    .filter(F.col("qty_percent_rank_tier").isin("C", "B"))
    .select(
        "qty_rank",
        "qty_percent_rank_tier",
        "total_qty_rank",
        "min_qty_percent_rank",
        "max_qty",
        "qty_percent_rank_to_prev_qty_avg_3_prior_ratio",
        F.abs(F.col("total_qty_rank") - 28).alias("total_qty_rank_distance"),
    )
    .orderBy(F.col("min_qty_percent_rank").desc())
)
