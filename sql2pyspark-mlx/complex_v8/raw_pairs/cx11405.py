from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_running = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_warehouse_city_by_war = Window.partitionBy("sku_id", "warehouse_city").orderBy("warehouse_capacity", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_sku_id_warehouse_city_by_war_2 = Window.partitionBy("sku_id", "warehouse_city").orderBy("warehouse_capacity")
w_warehouse_id_by_warehouse_ca = Window.partitionBy("warehouse_id").orderBy(F.col("warehouse_capacity").desc(), "id")
w_move_type = Window.partitionBy("move_type")
w_sku_id_by_moved_at_plus_30m = Window.partitionBy("sku_id").orderBy("moved_at_plus_30m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_moved_at_plus_30m_2 = Window.partitionBy("sku_id").orderBy("moved_at_plus_30m", "id")
w_move_type_by_warehouse_capac = Window.partitionBy("move_type").orderBy("warehouse_capacity", "id")
w_sku_id_by_moved_at_plus_30m_3 = Window.partitionBy("sku_id").orderBy("moved_at_plus_30m", "id").rowsBetween(-6, Window.currentRow)
w_moved_at_plus_30m_rank_by_fi = Window.partitionBy("moved_at_plus_30m_rank").orderBy(F.col("first_moved_at_plus_30m").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_plus_30m_rank_by_fi_2 = Window.partitionBy("moved_at_plus_30m_rank").orderBy(F.col("first_moved_at_plus_30m").desc(), "id")
w_all_by_warehouse_capacity = Window.orderBy(F.col("warehouse_capacity").asc_nulls_last(), "id")
w_moved_at_plus_30m_rank_by_un = Window.partitionBy("moved_at_plus_30m_rank").orderBy("unit_price_vs_move_type_avg", "id")
w_moved_at_plus_30m_rank = Window.partitionBy("moved_at_plus_30m_rank")
w_moved_at_plus_30m_rank_by_un_2 = Window.partitionBy("moved_at_plus_30m_rank").orderBy("unit_price_vs_move_type_avg")

base_moves = (
    df_stock_moves
    .filter(F.col("qty") > 137)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
    )
)

with_details = (
    base_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.length(F.trim(F.col("w.wh_code"))) >= 4), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.moved_at_plus_30m",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

windowed = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "warehouse_city",
        "warehouse_capacity",
        F.last("warehouse_capacity", ignorenulls=True).over(w_all_by_moved_at_running).alias("last_known_warehouse_capacity"),
        F.first("moved_at_plus_30m").over(w_sku_id_warehouse_city_by_war).alias("first_moved_at_plus_30m"),
        F.rank().over(w_all_by_moved_at_ties).alias("moved_at_rank"),
        F.dense_rank().over(w_sku_id_warehouse_city_by_war_2).alias("warehouse_capacity_dense_rank"),
        (F.col("unit_price") - F.lag("unit_price").over(w_warehouse_id_by_warehouse_ca)).alias("unit_price_change"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type), 2).alias("unit_price_vs_move_type_avg"),
        F.first("moved_at_plus_30m").over(w_sku_id_by_moved_at_plus_30m).alias("first_moved_at_plus_30m_2"),
        F.rank().over(w_sku_id_by_moved_at_plus_30m_2).alias("moved_at_plus_30m_rank"),
        F.row_number().over(w_move_type_by_warehouse_capac).alias("warehouse_capacity_seq"),
        F.round(F.avg("unit_price").over(w_sku_id_by_moved_at_plus_30m_3), 2).alias("unit_price_avg_7"),
        F.round(F.col("warehouse_capacity") / F.nullif(F.sum("warehouse_capacity").over(w_move_type), F.lit(0)), 4).alias("warehouse_capacity_share_of_move_type"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "sku_id",
        "warehouse_capacity",
        "moved_at_rank",
        "warehouse_capacity_dense_rank",
        "unit_price_vs_move_type_avg",
        "moved_at_plus_30m_rank",
        "warehouse_capacity_seq",
        F.sum("unit_price_change").over(w_moved_at_plus_30m_rank_by_fi).alias("running_unit_price_change"),
        F.row_number().over(w_moved_at_plus_30m_rank_by_fi_2).alias("seq_no"),
        F.dense_rank().over(w_all_by_warehouse_capacity).alias("warehouse_capacity_dense_rank_2"),
        F.dense_rank().over(w_moved_at_plus_30m_rank_by_un).alias("unit_price_vs_move_type_avg_dense_rank"),
        F.round(F.col("moved_at_rank") / F.nullif(F.sum("moved_at_rank").over(w_moved_at_plus_30m_rank), F.lit(0)), 4).alias("moved_at_rank_share_of_moved_at_plus_30m_rank"),
        F.dense_rank().over(w_moved_at_plus_30m_rank_by_un_2).alias("unit_price_vs_move_type_avg_dense_rank_2"),
        F.sum("unit_price_change").over(w_moved_at_plus_30m_rank).alias("moved_at_plus_30m_rank_sum_unit_price_change"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "sku_id",
        "warehouse_capacity",
        "moved_at_rank",
        "warehouse_capacity_dense_rank",
        "running_unit_price_change",
        "warehouse_capacity_dense_rank_2",
        "unit_price_vs_move_type_avg_dense_rank",
        "moved_at_rank_share_of_moved_at_plus_30m_rank",
        "unit_price_vs_move_type_avg_dense_rank_2",
        "moved_at_plus_30m_rank_sum_unit_price_change",
    )
    .orderBy(
        F.col("moved_at_plus_30m_rank_sum_unit_price_change").desc(),
        F.col("unit_price_vs_move_type_avg_dense_rank_2").desc_nulls_first(),
    )
)
