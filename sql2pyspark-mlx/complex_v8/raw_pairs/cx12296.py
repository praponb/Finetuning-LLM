from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_is_active_moved_at_to_da = Window.partitionBy("sku_is_active", F.to_date("moved_at")).orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_is_active = Window.partitionBy("sku_is_active")
w_all_by_qty_ties = Window.orderBy(F.col("qty").asc_nulls_last())
w_sku_is_active_moved_at_to_da_2 = Window.partitionBy("sku_is_active", F.to_date("moved_at")).orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_is_active_moved_at_to_da_3 = Window.partitionBy("sku_is_active", F.to_date("moved_at")).orderBy("qty", "id")
w_sku_category_sku_id_by_sku_u = Window.partitionBy("sku_category", "sku_id").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty_running = Window.orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_prior_1 = Window.orderBy("qty", "id").rowsBetween(-1, -1)
w_sku_is_active_move_type = Window.partitionBy("sku_is_active", "move_type")

prepared_moves = (
    df_stock_moves
    .filter((F.col("operator") != "op_dew") & (F.col("move_type").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.when(F.col("move_type").isin("TRANSFER", "OUTBOUND"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

combined_details_sku = (
    prepared_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.move_type_group",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.category").alias("sku_category"),
    )
)

sequenced = (
    combined_details_sku
    .select(
        "*",
        F.first("qty").over(w_sku_is_active_moved_at_to_da).alias("first_qty"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_is_active), 2).alias("qty_vs_sku_is_active_avg"),
        F.round(F.percent_rank().over(w_all_by_qty_ties), 3).alias("qty_percent_rank"),
        F.count(F.lit(1)).over(w_sku_is_active_moved_at_to_da_2).alias("running_count"),
        F.lead("qty", 2).over(w_sku_is_active_moved_at_to_da_3).alias("next2_qty"),
        F.nth_value("moved_at", 2).over(w_sku_category_sku_id_by_sku_u).alias("second_moved_at"),
        F.min("sku_unit_cost").over(w_sku_is_active).alias("sku_is_active_min_sku_unit_cost"),
        F.last("moved_at", ignorenulls=True).over(w_all_by_qty_running).alias("last_known_moved_at"),
        F.round(F.avg("qty").over(w_all_by_qty_prior_1), 2).alias("qty_avg_1_prior"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_is_active_move_type), F.lit(0)), 4).alias("qty_share_of_sku_is_active_move_type"),
        F.count(F.lit(1)).over(w_sku_is_active_move_type).alias("rows_in_sku_is_active_move_type"),
    )
)

enriched = (
    sequenced
    .filter(F.col("qty_percent_rank") < 0.9)
    .select(
        "*",
        F.coalesce(F.col("running_count"), F.lit(0)).alias("running_count_filled"),
        (F.col("running_count") - F.col("qty_vs_sku_is_active_avg")).alias("running_count_minus_qty_vs_sku_is_active_avg"),
        F.substring(F.trim(F.col("move_type_group")), 1, 2).alias("move_type_group_prefix"),
    )
)

rollup_base = (
    enriched
    .groupBy("sku_id")
    .agg(
        F.min("sku_unit_cost").alias("min_sku_unit_cost"),
        F.max("qty_vs_sku_is_active_avg").alias("max_qty_vs_sku_is_active_avg"),
        F.sum(F.when(~F.col("sku_is_active"), F.col("qty_share_of_sku_is_active_move_type"))).alias("qty_share_of_sku_is_active_move_type_sku_is_acti"),
    )
)

result = (
    rollup_base
    .filter(F.col("max_qty_vs_sku_is_active_avg").between(-450.0, 450.0))
    .select(
        "sku_id",
        "max_qty_vs_sku_is_active_avg",
        "qty_share_of_sku_is_active_move_type_sku_is_acti",
        F.coalesce(F.col("min_sku_unit_cost"), F.lit(0.0)).alias("min_sku_unit_cost_filled"),
    )
    .orderBy("min_sku_unit_cost_filled", "sku_id")
    .limit(10)
)
