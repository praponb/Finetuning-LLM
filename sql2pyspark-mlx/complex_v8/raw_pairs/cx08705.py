from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_opened_on_month_sk = Window.partitionBy(F.month("warehouse_opened_on"), "sku_id").orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(-1, -1)
w_move_type_sku_category_by_mo = Window.partitionBy("move_type", "sku_category").orderBy("moved_at")
w_warehouse_id_moved_at_month = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy("warehouse_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_moved_at_day = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at_day_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_category_sku_id = Window.partitionBy("sku_category", "sku_id")
w_warehouse_id_by_warehouse_op = Window.partitionBy("warehouse_id").orderBy("warehouse_opened_on", "id").rowsBetween(-1, -1)
w_warehouse_id_by_moved_at_day_2 = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at_day_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_category = Window.partitionBy("sku_category")

filtered_moves = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day_2"),
    )
)

with_details_warehouse = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.moved_at_day",
        "s.moved_at_day_2",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

sequenced = (
    with_details_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "moved_at_day",
        "moved_at_day_2",
        "sku_unit_cost",
        F.round(F.avg("sku_unit_cost").over(w_warehouse_opened_on_month_sk), 2).alias("sku_unit_cost_avg_1_prior"),
        F.round(F.cume_dist().over(w_move_type_sku_category_by_mo), 3).alias("moved_at_cume_dist"),
        F.min("qty").over(w_warehouse_id_moved_at_month).alias("min_qty_so_far"),
        F.last("moved_at_day_2", ignorenulls=True).over(w_warehouse_id_by_moved_at_day).alias("last_known_moved_at_day_2"),
        F.size(F.collect_set("move_type").over(w_sku_category_sku_id)).alias("distinct_move_type_in_sku_category_sku_id"),
        F.sum("qty").over(w_warehouse_id_by_warehouse_op).alias("qty_sum_1_prior"),
        F.size(F.collect_set("warehouse_id").over(w_sku_category_sku_id)).alias("distinct_warehouse_id_in_sku_category_sku_id"),
        F.last("moved_at").over(w_warehouse_id_by_moved_at_day_2).alias("last_moved_at"),
        F.round(F.col("sku_unit_cost") / F.nullif(F.sum("sku_unit_cost").over(w_sku_category), F.lit(0)), 4).alias("sku_unit_cost_share_of_sku_category"),
    )
)

enriched = (
    sequenced
    .withColumn("moved_at_day_plus_2h", F.col("moved_at_day") + F.expr("INTERVAL 2 HOURS"))
)

result = (
    enriched
    .filter(F.col("qty") <= 387)
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "moved_at_day",
        "sku_unit_cost",
        "sku_unit_cost_avg_1_prior",
        "moved_at_cume_dist",
        "min_qty_so_far",
        "last_known_moved_at_day_2",
        "distinct_move_type_in_sku_category_sku_id",
        "distinct_warehouse_id_in_sku_category_sku_id",
        "moved_at_day_plus_2h",
        F.date_format(F.col("moved_at_day_2"), "yyyy-MM-dd").alias("moved_at_day_2_label"),
    )
    .orderBy(F.col("distinct_move_type_in_sku_category_sku_id").asc_nulls_last())
)
