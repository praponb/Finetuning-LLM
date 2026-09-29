from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_by_moved_at_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id = Window.partitionBy("sku_id")
w_sku_id_by_moved_at_full = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_moving_6 = Window.orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w_sku_id_by_sku_unit_cost_prio = Window.partitionBy("sku_id").orderBy("sku_unit_cost", "id").rowsBetween(-6, -1)
w_sku_id_by_sku_unit_cost_full = Window.partitionBy("sku_id").orderBy("sku_unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_sku_unit_cost = Window.partitionBy("operator").orderBy(F.col("sku_unit_cost").desc(), "id")
w_all_by_unit_price_full = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_sku_unit_cost_fu = Window.partitionBy("operator").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_ties = Window.orderBy("unit_price")

recent = (
    df_stock_moves
    .filter(F.col("sku_id") <= 17)
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

enriched = (
    recent.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.lead("qty", 3).over(w_sku_id_by_moved_at).alias("next3_qty"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.sum("qty").over(w_sku_id_by_moved_at_running).alias("running_qty"),
        F.max("qty").over(w_sku_id).alias("sku_id_max_qty"),
        F.nth_value("sku_weight_kg", 2).over(w_sku_id_by_moved_at_full).alias("second_sku_weight_kg"),
        F.round(F.avg("qty").over(w_all_by_unit_price_moving_6), 2).alias("qty_avg_7"),
        F.round(F.avg("sku_weight_kg").over(w_sku_id_by_sku_unit_cost_prio), 2).alias("sku_weight_kg_avg_6_prior"),
        F.first("sku_weight_kg").over(w_sku_id_by_sku_unit_cost_full).alias("first_sku_weight_kg"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id), 2).alias("qty_vs_sku_id_avg"),
        (F.col("qty") - F.lag("qty").over(w_operator_by_sku_unit_cost)).alias("qty_change"),
        F.round(F.col("sku_unit_cost") / F.nullif(F.sum("sku_unit_cost").over(w_sku_id), F.lit(0)), 4).alias("sku_unit_cost_share_of_sku_id"),
        F.first("unit_price").over(w_all_by_unit_price_full).alias("first_unit_price"),
        F.nth_value("moved_at", 2).over(w_operator_by_sku_unit_cost_fu).alias("second_moved_at"),
        F.max("sku_weight_kg").over(w_all_by_unit_price_moving_6).alias("sku_weight_kg_max_7"),
        F.first("moved_at", ignorenulls=True).over(w_sku_id_by_moved_at_running).alias("first_known_moved_at"),
        F.round(F.percent_rank().over(w_all_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
    )
)

scored = (
    with_trends
    .withColumn("greatest_second_sku_weight_kg_sku_weight_kg", F.greatest(F.col("second_sku_weight_kg"), F.col("sku_weight_kg")))
)

result = (
    scored
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "sku_weight_kg",
        "next3_qty",
        "running_qty",
        "sku_id_max_qty",
        "qty_avg_7",
        "sku_weight_kg_avg_6_prior",
        "first_sku_weight_kg",
        "qty_vs_sku_id_avg",
        "qty_change",
        "sku_unit_cost_share_of_sku_id",
        "first_unit_price",
        "second_moved_at",
        "first_known_moved_at",
        "unit_price_percent_rank",
        "greatest_second_sku_weight_kg_sku_weight_kg",
    )
    .orderBy(F.col("sku_weight_kg").asc_nulls_last(), "id")
    .limit(20)
)
