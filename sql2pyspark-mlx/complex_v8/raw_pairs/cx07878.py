from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_filled = Window.orderBy(F.col("unit_price_filled").asc_nulls_last(), "id")
w_moved_at_year_sku_id_by_unit = Window.partitionBy(F.year("moved_at"), "sku_id").orderBy("unit_price_filled", "id")
w_all_by_unit_price_filled_tie = Window.orderBy(F.col("unit_price_filled").asc_nulls_last())
w_sku_is_active = Window.partitionBy("sku_is_active")
w_sku_is_active_operator_by_un = Window.partitionBy("sku_is_active", "operator").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_is_active_by_moved_at_fu = Window.partitionBy("sku_is_active").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_year_sku_id_by_unit_2 = Window.partitionBy(F.year("moved_at"), "sku_id").orderBy("unit_price_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator = Window.partitionBy("operator")
w_sku_is_active_warehouse_id_b = Window.partitionBy("sku_is_active", "warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_is_active_operator_by_un_2 = Window.partitionBy("sku_is_active", "operator").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_is_active_warehouse_id_b_2 = Window.partitionBy("sku_is_active", "warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-3, -1)

scoped_moves = (
    df_stock_moves
    .filter((F.col("unit_price") >= 124.1) & (F.col("sku_id") >= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
    )
)

enriched = (
    scoped_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_filled",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.city").alias("warehouse_city"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_unit_price_filled), 3).alias("unit_price_filled_percent_rank"),
        F.dense_rank().over(w_moved_at_year_sku_id_by_unit).alias("unit_price_filled_dense_rank"),
        F.rank().over(w_all_by_unit_price_filled_tie).alias("unit_price_filled_rank"),
        F.count(F.lit(1)).over(w_sku_is_active).alias("rows_in_sku_is_active"),
        F.nth_value("moved_at", 2).over(w_sku_is_active_operator_by_un).alias("second_moved_at"),
        F.nth_value("unit_price_filled", 2).over(w_sku_is_active_by_moved_at_fu).alias("second_unit_price_filled"),
        F.first("unit_price_filled", ignorenulls=True).over(w_moved_at_year_sku_id_by_unit_2).alias("first_known_unit_price_filled"),
        F.min("unit_price_filled").over(w_sku_is_active).alias("sku_is_active_min_unit_price_filled"),
        F.lead("unit_price_filled").over(w_all_by_unit_price_filled).alias("next_unit_price_filled"),
        F.sum("unit_price").over(w_operator).alias("operator_sum_unit_price"),
        F.nth_value("unit_price_filled", 2).over(w_sku_is_active_warehouse_id_b).alias("second_unit_price_filled_2"),
        F.sum("unit_price").over(w_sku_is_active_operator_by_un_2).alias("running_unit_price"),
        F.round(F.col("unit_price_filled") - F.avg("unit_price_filled").over(w_sku_is_active), 2).alias("unit_price_filled_vs_sku_is_active_avg"),
        F.round(F.percent_rank().over(w_all_by_unit_price_filled_tie), 3).alias("unit_price_filled_percent_rank_2"),
        F.max("unit_price_filled").over(w_sku_is_active_warehouse_id_b_2).alias("unit_price_filled_max_3_prior"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "operator",
        "unit_price",
        "unit_price_filled",
        "sku_is_active",
        "warehouse_city",
        "unit_price_filled_rank",
        "rows_in_sku_is_active",
        "second_moved_at",
        "second_unit_price_filled",
        "first_known_unit_price_filled",
        "next_unit_price_filled",
        "operator_sum_unit_price",
        "second_unit_price_filled_2",
        "running_unit_price",
        "unit_price_filled_vs_sku_is_active_avg",
        "unit_price_filled_percent_rank_2",
        "unit_price_filled_max_3_prior",
    )
    .orderBy(F.col("unit_price_filled_max_3_prior").asc())
)
