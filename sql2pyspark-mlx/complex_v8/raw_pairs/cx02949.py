from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_sku_id_by_unit_pri = Window.partitionBy("move_type", "sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_moved_at_year = Window.partitionBy("move_type", F.year("moved_at"))
w_distinct_operator_in_move_ty = Window.partitionBy("distinct_operator_in_move_type_moved_at_year")
w_all_by_last_moved_at = Window.orderBy(F.col("last_moved_at").asc_nulls_last(), "id")

scoped = df_stock_moves.filter(F.col("unit_price") >= 164.1)

sku_id_stats = (
    scoped
    .groupBy("sku_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

compared = (
    scoped.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.unit_price") > F.col("g.avg_unit_price"))
    .select(
        "s.*",
        "g.avg_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.avg_unit_price"), 2).alias("unit_price_minus_avg"),
    )
)

joined = (
    compared.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.lower(F.col("w.wh_code")).like("%nx%"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_minus_avg",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

move_type_stats = (
    joined
    .groupBy("move_type")
    .agg(F.percentile_approx("qty", 0.5).alias("median_qty"))
)

compared_2 = (
    joined.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.qty") >= F.col("g.median_qty"))
    .select(
        "s.*",
        "g.median_qty",
        F.round(F.col("s.qty") - F.col("g.median_qty"), 2).alias("qty_minus_median"),
    )
)

windowed = (
    compared_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_minus_avg",
        "warehouse_wh_code",
        "warehouse_capacity",
        "qty_minus_median",
        F.last("moved_at").over(w_move_type_sku_id_by_unit_pri).alias("last_moved_at"),
        F.size(F.collect_set("operator").over(w_move_type_moved_at_year)).alias("distinct_operator_in_move_type_moved_at_year"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "operator",
        "unit_price",
        "warehouse_wh_code",
        "warehouse_capacity",
        "last_moved_at",
        F.size(F.collect_set("move_type").over(w_distinct_operator_in_move_ty)).alias("distinct_move_type_in_distinct_operator_in_move_"),
        F.max("qty_minus_median").over(w_distinct_operator_in_move_ty).alias("distinct_operator_in_move_type_moved_at_year_max"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_all_by_last_moved_at))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.round(F.col("qty_minus_median") - F.avg("qty_minus_median").over(w_distinct_operator_in_move_ty), 2).alias("qty_minus_median_vs_distinct_operator_in_move_ty"),
    )
)

result = (
    with_trends
    .select(
        "id",
        F.col("distinct_move_type_in_distinct_operator_in_move_").alias("distinct_move_type_in_distinct_operator_in_move__2"),
        F.col("warehouse_capacity").alias("warehouse_capacity_v"),
    )
    .unpivot(
        ids=["id"],
        values=["distinct_move_type_in_distinct_operator_in_move__2", "warehouse_capacity_v"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
