from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_sku_unit_cost_ties = Window.partitionBy("sku_id").orderBy("sku_unit_cost")
w_move_type_by_unit_price_runn = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_all_by_unit_price_moving_2 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(-2, Window.currentRow)
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_sku_id_by_sku_unit_cost_full = Window.partitionBy("sku_id").orderBy("sku_unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_unit_price_ties = Window.partitionBy("sku_id").orderBy("unit_price")
w_move_type_by_unit_price_ties = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc_nulls_first())
w_warehouse_id_unit_price_dens = Window.partitionBy("warehouse_id", "unit_price_dense_rank_2").orderBy("minutes_since_prev_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id = Window.partitionBy("warehouse_id")

cleaned_moves = (
    df_stock_moves
    .filter(F.col("qty") > 191)
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

with_details = (
    cleaned_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

windowed = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "warehouse_wh_code",
        "sku_unit_cost",
        F.dense_rank().over(w_sku_id_by_sku_unit_cost_ties).alias("sku_unit_cost_dense_rank"),
        F.first("moved_at", ignorenulls=True).over(w_move_type_by_unit_price_runn).alias("first_known_moved_at"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id_sku_id), F.lit(0)), 4).alias("unit_price_share_of_warehouse_id_sku_id"),
        F.round(F.avg("sku_unit_cost").over(w_all_by_unit_price_moving_2), 2).alias("sku_unit_cost_avg_3"),
        F.count(F.lit(1)).over(w_warehouse_id_move_type).alias("rows_in_warehouse_id_move_type"),
        F.dense_rank().over(w_sku_id_by_unit_price).alias("unit_price_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_unit_price), 3).alias("unit_price_cume_dist"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_move_type), 2).alias("unit_price_vs_warehouse_id_move_type_avg"),
        F.last("unit_price").over(w_sku_id_by_sku_unit_cost_full).alias("last_unit_price"),
        F.dense_rank().over(w_sku_id_by_unit_price_ties).alias("unit_price_dense_rank_2"),
        F.round(F.percent_rank().over(w_move_type_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
        F.lag("sku_unit_cost").over(w_sku_id_by_unit_price).alias("prev_sku_unit_cost"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_all_by_unit_price))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "sku_id",
        "moved_at",
        "first_known_moved_at",
        "sku_unit_cost_avg_3",
        "rows_in_warehouse_id_move_type",
        "unit_price_dense_rank",
        "unit_price_cume_dist",
        "last_unit_price",
        "unit_price_percent_rank",
        "prev_sku_unit_cost",
        F.nth_value("prev_sku_unit_cost", 2).over(w_warehouse_id_unit_price_dens).alias("second_prev_sku_unit_cost"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
        F.round(F.col("sku_unit_cost_dense_rank") - F.avg("sku_unit_cost_dense_rank").over(w_warehouse_id), 2).alias("sku_unit_cost_dense_rank_vs_warehouse_id_avg"),
        F.max("unit_price_percent_rank").over(w_warehouse_id).alias("warehouse_id_max_unit_price_percent_rank"),
        F.round(F.col("last_unit_price") - F.avg("last_unit_price").over(w_warehouse_id), 2).alias("last_unit_price_vs_warehouse_id_avg"),
    )
)

result = (
    sequenced
    .filter(F.col("rows_in_warehouse_id_move_type") >= 7)
    .select(
        "id",
        "sku_id",
        "first_known_moved_at",
        "rows_in_warehouse_id_move_type",
        "unit_price_dense_rank",
        "unit_price_cume_dist",
        "last_unit_price",
        "second_prev_sku_unit_cost",
        "distinct_sku_id_in_warehouse_id",
        "sku_unit_cost_dense_rank_vs_warehouse_id_avg",
        "warehouse_id_max_unit_price_percent_rank",
        "last_unit_price_vs_warehouse_id_avg",
        F.when(F.col("unit_price_dense_rank") >= 7, "HIGH").when(F.col("unit_price_dense_rank") >= 5, "MEDIUM").otherwise("LOW").alias("unit_price_dense_rank_tier"),
    )
    .orderBy(
        F.col("warehouse_id_max_unit_price_percent_rank").desc(),
        F.col("unit_price_dense_rank_tier").desc_nulls_first(),
    )
)
