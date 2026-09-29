from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_moved_at_runnin = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_sku_is_active = Window.partitionBy("warehouse_id", "sku_is_active")
w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_is_active_by_qty_prior_6 = Window.partitionBy("sku_is_active").orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_operator_sku_id_by_sku_unit = Window.partitionBy("operator", "sku_id").orderBy(F.col("sku_unit_cost").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_move_type = Window.partitionBy("move_type")
w_sku_id_move_type_by_moved_at_2 = Window.partitionBy("sku_id", "move_type").orderBy("moved_at", "id").rowsBetween(-6, -1)
w_operator_sku_id_by_sku_unit_2 = Window.partitionBy("operator", "sku_id").orderBy(F.col("sku_unit_cost").desc_nulls_first(), "id")

cleaned = (
    df_stock_moves
    .filter((~F.col("operator").isin("op_eve", "op_dew")) | (F.col("moved_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("TRANSFER", "ADJUST"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.when(F.col("qty") >= 314, "A").when(F.col("qty") >= 111, "B").otherwise("C").alias("qty_tier"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.qty_tier",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

with_windows = (
    with_parent
    .select(
        "*",
        F.sum("unit_price").over(w_move_type_by_moved_at_runnin).alias("running_unit_price"),
        F.size(F.collect_set("operator").over(w_warehouse_id_sku_is_active)).alias("distinct_operator_in_warehouse_id_sku_is_active"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_sku_is_active), 2).alias("unit_price_vs_warehouse_id_sku_is_active_avg"),
        F.sum("sku_unit_cost").over(w_sku_id_move_type_by_moved_at).alias("running_sku_unit_cost"),
        F.round(F.avg("qty").over(w_sku_is_active_by_qty_prior_6), 2).alias("qty_avg_6_prior"),
        F.round(F.avg("unit_price").over(w_operator_sku_id_by_sku_unit), 2).alias("unit_price_avg_1_prior"),
        F.size(F.collect_set("qty_tier").over(w_move_type)).alias("distinct_qty_tier_in_move_type"),
        F.round(F.avg("unit_price").over(w_sku_id_move_type_by_moved_at_2), 2).alias("unit_price_avg_6_prior"),
        F.round(F.percent_rank().over(w_operator_sku_id_by_sku_unit_2), 3).alias("sku_unit_cost_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "qty_tier",
        "sku_labels",
        "sku_is_active",
        "sku_unit_cost",
        "running_unit_price",
        "distinct_operator_in_warehouse_id_sku_is_active",
        "unit_price_vs_warehouse_id_sku_is_active_avg",
        "running_sku_unit_cost",
        "unit_price_avg_1_prior",
        "distinct_qty_tier_in_move_type",
        F.coalesce(F.col("unit_price_avg_6_prior"), F.lit(0.0)).alias("unit_price_avg_6_prior_filled"),
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)
