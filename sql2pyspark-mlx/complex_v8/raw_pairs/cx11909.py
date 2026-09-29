from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_min_qty_minus_median = Window.orderBy(F.col("min_qty_minus_median").desc())
w_move_id = Window.partitionBy("move_id")
w_all_by_min_qty_minus_median_2 = Window.orderBy(F.col("min_qty_minus_median").desc(), "warehouse_id", "move_id")
w_move_id_warehouse_id_by_qty = Window.partitionBy("move_id", "warehouse_id").orderBy("qty_minus_median_to_qty_per_unit_price_ratio", "warehouse_id", "move_id")

cleaned_moves = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(1, 3)) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

filtered = (
    cleaned_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
    .filter(F.col("days_since_moved_at") <= 560)
)

warehouse_id_stats = (
    filtered
    .groupBy("warehouse_id")
    .agg(F.percentile_approx("qty", 0.5).alias("median_qty"))
)

outliers = (
    filtered.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.qty") >= F.col("g.median_qty"))
    .select(
        "s.*",
        "g.median_qty",
        F.round(F.col("s.qty") - F.col("g.median_qty"), 2).alias("qty_minus_median"),
    )
)

line_level = (
    outliers.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty_per_unit_price",
        "s.qty_minus_median",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

stats = (
    line_level
    .groupBy("warehouse_id", "move_id")
    .agg(
        F.array_sort(F.collect_set("move_operator")).alias("move_operator_list"),
        F.round(F.sum("qty_minus_median") / F.nullif(F.sum("qty_per_unit_price"), F.lit(0)), 3).alias("qty_minus_median_to_qty_per_unit_price_ratio"),
        F.min("qty_minus_median").alias("min_qty_minus_median"),
        F.count("*").alias("n_rows"),
        F.sum("qty_minus_median").alias("sum_qty_minus_median_check"),
    )
    .filter(F.col("sum_qty_minus_median_check") > -250.0)
    .drop("sum_qty_minus_median_check")
)

qualified = (
    stats
    .crossJoin(stats.agg(F.avg("n_rows").alias("overall_avg_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_avg_n_rows"))
    .drop("overall_avg_n_rows")
)

sequenced = (
    qualified
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_min_qty_minus_median), 3).alias("min_qty_minus_median_percent_rank"),
        F.count(F.lit(1)).over(w_move_id).alias("rows_in_move_id"),
        F.lag("n_rows", 3).over(w_all_by_min_qty_minus_median_2).alias("prev3_n_rows"),
        F.lead("n_rows", 2, 0).over(w_move_id_warehouse_id_by_qty).alias("next2_n_rows"),
    )
)

result = (
    sequenced
    .select(
        "warehouse_id",
        "move_id",
        "min_qty_minus_median",
        "qty_minus_median_to_qty_per_unit_price_ratio",
    )
    .unpivot(
        ids=["warehouse_id", "move_id"],
        values=["min_qty_minus_median", "qty_minus_median_to_qty_per_unit_price_ratio"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("warehouse_id", "move_id", "metric_name")
)
