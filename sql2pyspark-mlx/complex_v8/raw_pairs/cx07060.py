from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_operator = Window.partitionBy("move_type", "operator")
w_operator_by_qty_full = Window.partitionBy("operator").orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_qty_running = Window.partitionBy("operator").orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_qty_prior_2 = Window.partitionBy("operator").orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(-2, -1)

prepared = (
    df_stock_moves
    .filter((F.col("unit_price") >= 99.1) & (F.col("sku_id") >= 15))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "unit_price_plus_qty",
        F.count(F.lit(1)).over(w_move_type_operator).alias("rows_in_move_type_operator"),
        F.max("qty").over(w_move_type_operator).alias("move_type_operator_max_qty"),
        F.nth_value("moved_at", 2).over(w_operator_by_qty_full).alias("second_moved_at"),
        F.first("unit_price_plus_qty", ignorenulls=True).over(w_operator_by_qty_running).alias("first_known_unit_price_plus_qty"),
        F.sum("unit_price_plus_qty").over(w_operator_by_qty_prior_2).alias("unit_price_plus_qty_sum_2_prior"),
    )
)

rollup_base = (
    with_windows
    .groupBy(
        "sku_id",
        F.year("second_moved_at").alias("second_moved_at_year"),
        "operator",
    )
    .agg(
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.round(F.sum("qty") / F.nullif(F.sum("unit_price"), F.lit(0)), 3).alias("qty_to_unit_price_ratio"),
    )
)

result = (
    rollup_base
    .filter(F.col("sku_id").isNotNull())
    .select(
        "sku_id",
        "second_moved_at_year",
        "avg_qty",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
    .orderBy(F.col("operator_len").desc(), F.col("sku_id").desc_nulls_first())
)
