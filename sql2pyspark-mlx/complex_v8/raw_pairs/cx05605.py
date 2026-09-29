from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_is_active_warehouse_id_b = Window.partitionBy("sku_is_active", "warehouse_id").orderBy(F.col("warehouse_opened_on").desc(), "id")
w_sku_is_active_warehouse_id_b_2 = Window.partitionBy("sku_is_active", "warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_is_active_warehouse_id_b_3 = Window.partitionBy("sku_is_active", "warehouse_id").orderBy("moved_at", "id").rowsBetween(-3, -1)
w_move_type_by_warehouse_opene = Window.partitionBy("move_type").orderBy(F.col("warehouse_opened_on").desc(), "id")
w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_move_type_by_moved_at = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_all_by_warehouse_capacity_pr = Window.orderBy(F.col("warehouse_capacity").desc(), "id").rowsBetween(-1, -1)
w_sku_is_active_warehouse_id_b_4 = Window.partitionBy("sku_is_active", "warehouse_id").orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_warehouse_opened_o = Window.partitionBy("move_type", F.month("warehouse_opened_on"))
w_all_by_warehouse_capacity_ru = Window.orderBy(F.col("warehouse_capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_sku_is_active = Window.partitionBy("move_type", "sku_is_active")
w_move_type_by_warehouse_opene_2 = Window.partitionBy("move_type").orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(-3, Window.currentRow)
w_move_type_sku_is_active_by_w = Window.partitionBy("move_type", "sku_is_active").orderBy("warehouse_opened_on", "id")
w_sku_is_active_warehouse_id_b_5 = Window.partitionBy("sku_is_active", "warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_moves = (
    df_stock_moves
    .filter(F.col("qty") < 370)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

with_parent = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.upper(F.trim(F.col("sk.sku_code"))).like("E%")), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(F.col("sk.is_active"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "sk.sku_code",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_sku_is_active_warehouse_id_b))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.nth_value("warehouse_capacity", 2).over(w_sku_is_active_warehouse_id_b_2).alias("second_warehouse_capacity"),
        F.round(F.avg("warehouse_capacity").over(w_sku_is_active_warehouse_id_b_3), 2).alias("warehouse_capacity_avg_3_prior"),
        F.lead("warehouse_opened_on").over(w_move_type_by_warehouse_opene).alias("next_warehouse_opened_on"),
        F.count(F.lit(1)).over(w_move_type_warehouse_id).alias("rows_in_move_type_warehouse_id"),
        F.rank().over(w_move_type_by_moved_at).alias("moved_at_rank"),
        F.round(F.avg("qty").over(w_all_by_warehouse_capacity_pr), 2).alias("qty_avg_1_prior"),
        F.last("qty").over(w_sku_is_active_warehouse_id_b_4).alias("last_qty"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_move_type_warehouse_opened_o), 2).alias("warehouse_capacity_vs_move_type_warehouse_opened"),
        F.last("qty", ignorenulls=True).over(w_all_by_warehouse_capacity_ru).alias("last_known_qty"),
        F.count(F.lit(1)).over(w_move_type_sku_is_active).alias("rows_in_move_type_sku_is_active"),
        F.round(F.avg("warehouse_capacity").over(w_move_type_by_warehouse_opene_2), 2).alias("warehouse_capacity_avg_4"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_move_type_sku_is_active_by_w))) / 60, 1).alias("minutes_since_prev_moved_at_2"),
        F.min("warehouse_capacity").over(w_sku_is_active_warehouse_id_b_5).alias("min_warehouse_capacity_so_far"),
        F.round(F.col("warehouse_capacity") / F.nullif(F.sum("warehouse_capacity").over(w_move_type_warehouse_id), F.lit(0)), 4).alias("warehouse_capacity_share_of_move_type_warehouse_"),
    )
)

annotated = (
    with_trends
    .filter(F.col("move_type").isin("ADJUST", "TRANSFER"))
    .withColumn("sku_is_active_flag", F.when(F.col("sku_is_active"), 1).otherwise(0))
)

result = (
    annotated
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "sku_code",
        "sku_is_active",
        "warehouse_opened_on",
        "warehouse_capacity",
        "minutes_since_prev_moved_at",
        "second_warehouse_capacity",
        "warehouse_capacity_avg_3_prior",
        "rows_in_move_type_warehouse_id",
        "moved_at_rank",
        "qty_avg_1_prior",
        "last_qty",
        "warehouse_capacity_vs_move_type_warehouse_opened",
        "last_known_qty",
        "rows_in_move_type_sku_is_active",
        "warehouse_capacity_avg_4",
        "minutes_since_prev_moved_at_2",
        "min_warehouse_capacity_so_far",
        "warehouse_capacity_share_of_move_type_warehouse_",
        "sku_is_active_flag",
        F.when(F.col("qty_avg_1_prior") >= 300.4, "HIGH").when(F.col("qty_avg_1_prior") >= 200.6, "MEDIUM").otherwise("LOW").alias("qty_avg_1_prior_tier"),
    )
)
