from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month = Window.partitionBy("moved_at_month")
w_all_by_qty_plus_unit_price_p = Window.orderBy("qty_plus_unit_price", "id").rowsBetween(-6, -1)
w_all_by_qty_plus_unit_price_t = Window.orderBy("qty_plus_unit_price")
w_all_by_moved_at_month = Window.orderBy(F.col("moved_at_month").desc_nulls_first(), "id")
w_all_by_qty_plus_unit_price_r = Window.orderBy("qty_plus_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_moves = (
    df_stock_moves
    .filter(F.col("qty").between(45, 357))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)

derived = (
    base_moves
    .filter(F.col("warehouse_id") <= 3)
    .select(
        "id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "qty_plus_unit_price",
        F.lower(F.trim(F.col("moved_at_label"))).alias("moved_at_label_clean"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

move_type_or_unknown_stats = (
    derived
    .groupBy("move_type_or_unknown")
    .agg(F.percentile_approx("qty", 0.5).alias("median_qty"))
)

compared = (
    derived.alias("s")
    .join(move_type_or_unknown_stats.alias("g"), F.col("g.move_type_or_unknown") == F.col("s.move_type_or_unknown"), "inner")
    .filter(F.col("s.qty") >= F.col("g.median_qty"))
    .select(
        "s.*",
        "g.median_qty",
        F.round(F.col("s.qty") - F.col("g.median_qty"), 2).alias("qty_minus_median"),
    )
)

annotated = (
    compared
    .select(
        "id",
        "moved_at",
        "qty_plus_unit_price",
        "move_type_or_unknown",
        "median_qty",
        "qty_minus_median",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

tagged = (
    annotated
    .select(
        "id",
        "moved_at",
        "qty_plus_unit_price",
        "days_since_moved_at",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
        F.month(F.col("moved_at")).alias("moved_at_month"),
    )
)

with_trends = (
    tagged
    .select(
        "id",
        "moved_at",
        "qty_plus_unit_price",
        "days_since_moved_at",
        "moved_at_label",
        "moved_at_month",
        F.round(F.avg("moved_at_month").over(w_all_by_qty_plus_unit_price_p), 2).alias("moved_at_month_avg_6_prior"),
        F.round(F.cume_dist().over(w_all_by_qty_plus_unit_price_t), 3).alias("qty_plus_unit_price_cume_dist"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_all_by_moved_at_month))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.round(F.percent_rank().over(w_all_by_moved_at_month), 3).alias("moved_at_month_percent_rank"),
        F.max("days_since_moved_at").over(w_all_by_qty_plus_unit_price_r).alias("max_days_since_moved_at_so_far"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "moved_at",
        "qty_plus_unit_price",
        "moved_at_label",
        "moved_at_month",
        "moved_at_month_avg_6_prior",
        "qty_plus_unit_price_cume_dist",
        "max_days_since_moved_at_so_far",
        (F.col("moved_at_month") + F.col("qty_plus_unit_price")).alias("moved_at_month_plus_qty_plus_unit_price"),
    )
    .orderBy(
        F.col("qty_plus_unit_price_cume_dist").desc(),
        F.col("moved_at_month_plus_qty_plus_unit_price").desc(),
    )
)
