from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_qty_prior_3 = Window.partitionBy("operator").orderBy("qty", "id").rowsBetween(-3, -1)
w_moved_at_month_month_qty_tie = Window.partitionBy(F.month("moved_at_month"), "qty_tier").orderBy(F.col("total_unit_price").desc())
w_operator = Window.partitionBy("operator")
w_moved_at_month_month_qty_tie_2 = Window.partitionBy(F.month("moved_at_month"), "qty_tier").orderBy(F.col("median_unit_price").desc(), "operator", "qty_tier", "moved_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_stddev_unit_price_avg = Window.orderBy("stddev_unit_price_avg_3_prior", "operator", "qty_tier", "moved_at_month")
w_all_by_stddev_unit_price_avg_2 = Window.orderBy("stddev_unit_price_avg_3_prior", "operator", "qty_tier", "moved_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_moves = (
    df_stock_moves
    .filter((F.col("operator").isin("op_chan", "op_bee")) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_windows = (
    recent_moves
    .withColumn("last_moved_at", F.last("moved_at").over(w_all_by_moved_at_full))
    .withColumn("unit_price_avg_3_prior", F.round(F.avg("unit_price").over(w_operator_by_qty_prior_3), 2))
)

enriched = (
    with_windows
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_moved_at")).alias("days_since_last_moved_at"),
        F.when(F.col("move_type").isin("INBOUND", "OUTBOUND"), "group_a").when(F.col("move_type") == "TRANSFER", "group_b").otherwise("other").alias("move_type_group"),
        F.when(F.col("qty") >= 357, "large").when(F.col("qty") >= 102, "medium").otherwise("small").alias("qty_tier"),
    )
)

grouped = (
    enriched
    .groupBy(
        "operator",
        "qty_tier",
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
    )
    .agg(
        F.round(F.sum("unit_price"), 2).alias("total_unit_price"),
        F.sum("qty").alias("total_qty"),
        F.percentile_approx("unit_price", 0.5).alias("median_unit_price"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.round(F.stddev("unit_price_avg_3_prior"), 2).alias("stddev_unit_price_avg_3_prior"),
    )
)

screened = (
    grouped
    .crossJoin(grouped.agg(F.avg("avg_qty").alias("overall_avg_avg_qty")))
    .filter(F.col("avg_qty") > F.col("overall_avg_avg_qty"))
    .drop("overall_avg_avg_qty")
)

with_windows_2 = (
    screened
    .select(
        "*",
        F.dense_rank().over(w_moved_at_month_month_qty_tie).alias("total_unit_price_dense_rank"),
        F.round(F.col("avg_qty") / F.nullif(F.sum("avg_qty").over(w_operator), F.lit(0)), 4).alias("avg_qty_share_of_operator"),
        F.first("total_qty").over(w_moved_at_month_month_qty_tie_2).alias("first_total_qty"),
        F.round(F.percent_rank().over(w_all_by_stddev_unit_price_avg), 3).alias("stddev_unit_price_avg_3_prior_percent_rank"),
        F.rank().over(w_all_by_stddev_unit_price_avg).alias("stddev_unit_price_avg_3_prior_rank"),
        F.nth_value("median_unit_price", 2).over(w_all_by_stddev_unit_price_avg_2).alias("second_median_unit_price"),
    )
)

top_per_group = (
    with_windows_2
    .filter(F.col("stddev_unit_price_avg_3_prior_rank") == 1)
    .drop("stddev_unit_price_avg_3_prior_rank")
)

result = (
    top_per_group
    .rollup("total_unit_price_dense_rank", "operator", "qty_tier")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.round(F.sum("first_total_qty") / F.nullif(F.sum("stddev_unit_price_avg_3_prior_percent_rank"), F.lit(0)), 3).alias("first_total_qty_to_stddev_unit_price_avg_3_prior"),
    )
    .select(
        "total_unit_price_dense_rank",
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        F.coalesce(F.col("qty_tier"), F.lit("ALL")).alias("qty_tier"),
        "cnt",
        "distinct_operators",
        "first_total_qty_to_stddev_unit_price_avg_3_prior",
    )
    .orderBy(
        F.col("total_unit_price_dense_rank").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
        F.col("qty_tier").asc_nulls_last(),
    )
)
