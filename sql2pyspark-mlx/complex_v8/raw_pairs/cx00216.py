from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_is_active_by_wa = Window.partitionBy("operator", "sku_is_active").orderBy(F.col("warehouse_capacity").desc())
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_by_warehouse_ca = Window.partitionBy("warehouse_id").orderBy("warehouse_capacity", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_unit_price_f = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_warehouse_capacity_de = Window.orderBy("warehouse_capacity_dense_rank", "id")
w_all_by_warehouse_capacity_de_2 = Window.orderBy("warehouse_capacity_dense_rank")
w_sku_id_by_warehouse_capacity = Window.partitionBy("sku_id").orderBy("warehouse_capacity_vs_sku_id_avg", "id").rowsBetween(-1, Window.currentRow)
w_warehouse_capacity_dense_ran = Window.partitionBy("warehouse_capacity_dense_rank", "sku_is_active")
w_all_by_warehouse_capacity_de_3 = Window.orderBy("warehouse_capacity_dense_rank", "id").rowsBetween(-6, -1)

prepared_moves = df_stock_moves.filter(F.col("sku_id").isNotNull())

with_details_sku = (
    prepared_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

with_trends = (
    with_details_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "sku_is_active",
        F.dense_rank().over(w_operator_sku_is_active_by_wa).alias("warehouse_capacity_dense_rank"),
        (F.col("warehouse_capacity") - F.lag("warehouse_capacity").over(w_warehouse_id_by_unit_price)).alias("warehouse_capacity_change"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_operator), 2).alias("unit_price_vs_warehouse_id_operator_avg"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_sku_id), 2).alias("warehouse_capacity_vs_sku_id_avg"),
        F.first("warehouse_capacity").over(w_warehouse_id_by_warehouse_ca).alias("first_warehouse_capacity"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id_operator)).alias("distinct_sku_id_in_warehouse_id_operator"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_warehouse_id_operator), 2).alias("warehouse_capacity_vs_warehouse_id_operator_avg"),
        F.nth_value("warehouse_capacity", 2).over(w_warehouse_id_by_unit_price_f).alias("second_warehouse_capacity"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "sku_id",
        "sku_is_active",
        "warehouse_capacity_dense_rank",
        "warehouse_capacity_change",
        "unit_price_vs_warehouse_id_operator_avg",
        "warehouse_capacity_vs_sku_id_avg",
        "first_warehouse_capacity",
        "distinct_sku_id_in_warehouse_id_operator",
        "warehouse_capacity_vs_warehouse_id_operator_avg",
        (F.col("warehouse_capacity_change") - F.lag("warehouse_capacity_change").over(w_all_by_warehouse_capacity_de)).alias("warehouse_capacity_change_change"),
        F.round(F.cume_dist().over(w_all_by_warehouse_capacity_de_2), 3).alias("warehouse_capacity_dense_rank_cume_dist"),
        F.row_number().over(w_all_by_warehouse_capacity_de).alias("warehouse_capacity_dense_rank_seq"),
        F.round(F.avg("warehouse_capacity_change").over(w_sku_id_by_warehouse_capacity), 2).alias("warehouse_capacity_change_avg_2"),
        F.round(F.avg("second_warehouse_capacity").over(w_warehouse_capacity_dense_ran), 2).alias("warehouse_capacity_dense_rank_sku_is_active_avg_"),
        F.max("first_warehouse_capacity").over(w_all_by_warehouse_capacity_de_3).alias("first_warehouse_capacity_max_6_prior"),
    )
)

result = (
    windowed
    .select(
        "id",
        "sku_id",
        "sku_is_active",
        "warehouse_capacity_dense_rank",
        "warehouse_capacity_change",
        "unit_price_vs_warehouse_id_operator_avg",
        "warehouse_capacity_vs_sku_id_avg",
        "distinct_sku_id_in_warehouse_id_operator",
        "warehouse_capacity_vs_warehouse_id_operator_avg",
        "warehouse_capacity_change_change",
        "warehouse_capacity_dense_rank_cume_dist",
        "warehouse_capacity_change_avg_2",
        "warehouse_capacity_dense_rank_sku_is_active_avg_",
        "first_warehouse_capacity_max_6_prior",
    )
    .orderBy(
        F.col("warehouse_capacity_vs_sku_id_avg").desc(),
        "warehouse_capacity_vs_warehouse_id_operator_avg",
    )
)
