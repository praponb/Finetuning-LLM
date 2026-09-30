from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_warehouse_capacity = Window.orderBy("warehouse_capacity", "id")
w_sku_id = Window.partitionBy("sku_id")
w_sku_is_active_by_unit_price = Window.partitionBy("sku_is_active").orderBy("unit_price")
w_moved_at_plus_30m_year_by_mo = Window.partitionBy(F.year("moved_at_plus_30m")).orderBy(F.col("moved_at_plus_30m").desc())
w_warehouse_city_warehouse_id = Window.partitionBy("warehouse_city", "warehouse_id")
w_warehouse_id_operator_by_mov = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("moved_at_plus_30m").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_warehouse_capacity_pr = Window.orderBy("warehouse_capacity", "id").rowsBetween(-6, -1)
w_moved_at_plus_30m_year_by_mo_2 = Window.partitionBy(F.year("moved_at_plus_30m")).orderBy(F.col("moved_at_plus_30m").desc(), "id")
w_moved_at_plus_30m_year_by_mo_3 = Window.partitionBy(F.year("moved_at_plus_30m")).orderBy(F.col("moved_at_plus_30m").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_moves = (
    df_stock_moves
    .filter((F.col("operator") == "op_chan") | (F.col("warehouse_id") <= 4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
    )
)

combined_details = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.moved_at_plus_30m",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

metrics = (
    combined_details
    .select(
        "*",
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_warehouse_capacity)).alias("unit_price_change"),
        F.max("unit_price").over(w_sku_id).alias("sku_id_max_unit_price"),
        F.rank().over(w_sku_is_active_by_unit_price).alias("unit_price_rank"),
        F.round(F.cume_dist().over(w_sku_is_active_by_unit_price), 3).alias("unit_price_cume_dist"),
        F.round(F.cume_dist().over(w_moved_at_plus_30m_year_by_mo), 3).alias("moved_at_plus_30m_cume_dist"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_warehouse_city_warehouse_id), 2).alias("warehouse_capacity_vs_warehouse_city_warehouse_i"),
        F.max("warehouse_capacity").over(w_warehouse_id_operator_by_mov).alias("max_warehouse_capacity_so_far"),
        F.round(F.avg("warehouse_capacity").over(w_all_by_warehouse_capacity_pr), 2).alias("warehouse_capacity_avg_6_prior"),
        F.round(F.cume_dist().over(w_moved_at_plus_30m_year_by_mo_2), 3).alias("moved_at_plus_30m_cume_dist_2"),
        F.round(F.avg("unit_price").over(w_sku_id), 2).alias("sku_id_avg_unit_price"),
        F.min("warehouse_capacity").over(w_warehouse_city_warehouse_id).alias("warehouse_city_warehouse_id_min_warehouse_capaci"),
        F.max("warehouse_capacity").over(w_moved_at_plus_30m_year_by_mo_3).alias("max_warehouse_capacity_so_far_2"),
        F.ntile(3).over(w_moved_at_plus_30m_year_by_mo_2).alias("moved_at_plus_30m_tercile"),
        F.dense_rank().over(w_moved_at_plus_30m_year_by_mo_2).alias("moved_at_plus_30m_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "sku_id",
        "unit_price",
        "moved_at_plus_30m",
        "warehouse_capacity",
        "sku_labels",
        "sku_is_active",
        "sku_id_max_unit_price",
        "unit_price_rank",
        "unit_price_cume_dist",
        "moved_at_plus_30m_cume_dist",
        "warehouse_capacity_vs_warehouse_city_warehouse_i",
        "max_warehouse_capacity_so_far",
        "moved_at_plus_30m_cume_dist_2",
        "sku_id_avg_unit_price",
        "warehouse_city_warehouse_id_min_warehouse_capaci",
        "max_warehouse_capacity_so_far_2",
        "moved_at_plus_30m_tercile",
        "moved_at_plus_30m_dense_rank",
    )
)
