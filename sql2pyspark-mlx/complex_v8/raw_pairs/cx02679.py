from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator_by_lea = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("least_unit_price_qty").desc(), "id").rowsBetween(-2, -1)
w_warehouse_id_operator_by_lea_2 = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("least_unit_price_qty").desc(), "id")
w_operator = Window.partitionBy("operator")
w_warehouse_id_operator_by_lea_3 = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("least_unit_price_qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_stock_moves
    .filter(F.col("operator") == "op_dew")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

with_trends = (
    recent
    .select(
        "*",
        F.max("qty_filled").over(w_warehouse_id_operator_by_lea).alias("qty_filled_max_2_prior"),
        F.lag("unit_price_per_qty").over(w_warehouse_id_operator_by_lea_2).alias("prev_unit_price_per_qty"),
        F.round(F.col("qty") - F.avg("qty").over(w_operator), 2).alias("qty_vs_operator_avg"),
        F.sum("qty").over(w_warehouse_id_operator_by_lea_3).alias("running_qty"),
        F.max("unit_price_per_qty").over(w_operator).alias("operator_max_unit_price_per_qty"),
        F.size(F.collect_set("sku_id").over(w_operator)).alias("distinct_sku_id_in_operator"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.capacity") < 11768)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.least_unit_price_qty",
        "s.unit_price_per_qty",
        "s.prev_unit_price_per_qty",
        "s.qty_vs_operator_avg",
        "s.running_qty",
        "s.operator_max_unit_price_per_qty",
        "s.distinct_sku_id_in_operator",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

enriched = (
    joined
    .filter(F.col("sku_id").isNotNull())
    .select(
        "*",
        F.year(F.col("warehouse_opened_on")).alias("warehouse_opened_on_year"),
        (F.col("unit_price_per_qty") + F.col("unit_price")).alias("unit_price_per_qty_plus_unit_price"),
    )
)

result = (
    enriched
    .groupBy("warehouse_opened_on_year")
    .pivot("move_type", ["ADJUST", "OUTBOUND"])
    .agg(F.count("unit_price_per_qty_plus_unit_price").alias("count_v"), F.max("unit_price_per_qty_plus_unit_price").alias("max_v"))
)
