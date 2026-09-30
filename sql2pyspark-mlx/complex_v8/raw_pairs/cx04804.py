from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_sku_weight_kg_running = Window.orderBy(F.col("sku_weight_kg").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price_ties = Window.orderBy("unit_price")
w_sku_id_by_sku_weight_kg_prio = Window.partitionBy("sku_id").orderBy("sku_weight_kg", "id").rowsBetween(-3, -1)
w_all_by_sku_weight_kg_full = Window.orderBy(F.col("sku_weight_kg").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_unit_price = Window.partitionBy("move_type").orderBy("unit_price", "id")
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_sku_is_active_by_qty = Window.partitionBy("sku_is_active").orderBy(F.col("qty").desc(), "id")
w_sku_id = Window.partitionBy("sku_id")
w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_sku_weight_kg = Window.orderBy(F.col("sku_weight_kg").desc_nulls_first(), "id")

base_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
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
    base_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (~F.array_contains(F.col("sk.labels"), "fragile")), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.sum("sku_weight_kg").over(w_all_by_sku_weight_kg_running).alias("running_sku_weight_kg"),
        F.round(F.percent_rank().over(w_all_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
        F.round(F.avg("unit_price").over(w_sku_id_by_sku_weight_kg_prio), 2).alias("unit_price_avg_3_prior"),
        F.first("unit_price").over(w_all_by_sku_weight_kg_full).alias("first_unit_price"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_move_type_by_unit_price))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_warehouse_id), 2).alias("unit_price_vs_sku_id_warehouse_id_avg"),
        F.round(F.percent_rank().over(w_sku_is_active_by_qty), 3).alias("qty_percent_rank"),
        F.round(F.avg("sku_weight_kg").over(w_sku_id), 2).alias("sku_id_avg_sku_weight_kg"),
        F.count(F.lit(1)).over(w_all_by_unit_price_running).alias("running_count"),
        F.lead("moved_at").over(w_all_by_sku_weight_kg).alias("next_moved_at"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.round(F.cume_dist().over(w_all_by_sku_weight_kg), 3).alias("sku_weight_kg_cume_dist"),
        F.count(F.lit(1)).over(w_sku_id_warehouse_id).alias("rows_in_sku_id_warehouse_id"),
    )
)

rollup_base = (
    ranked
    .groupBy(
        F.year("next_moved_at").alias("next_moved_at_year"),
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
    )
    .agg(
        F.sum("sku_weight_kg_cume_dist").alias("total_sku_weight_kg_cume_dist"),
        F.round(F.stddev("qty_percent_rank"), 2).alias("stddev_qty_percent_rank"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = rollup_base.select("next_moved_at_year", "moved_at_month", "total_sku_weight_kg_cume_dist")
