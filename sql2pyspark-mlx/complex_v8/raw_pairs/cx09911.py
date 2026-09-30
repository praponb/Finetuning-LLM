from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_all_by_qty_moving_3 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_operator_by_sku_weight_kg_ru = Window.partitionBy("operator").orderBy("sku_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_sku_weight_kg = Window.partitionBy("operator").orderBy("sku_weight_kg", "id")
w_warehouse_id_by_sku_weight_k = Window.partitionBy("warehouse_id").orderBy("sku_weight_kg", "id").rowsBetween(-1, -1)
w_operator_warehouse_id_by_mov = Window.partitionBy("operator", "warehouse_id").orderBy("moved_at", "id")
w_sku_id_by_unit_price_full = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_sku_weight_k_2 = Window.partitionBy("warehouse_id").orderBy("sku_weight_kg")
w_operator_by_sku_weight_kg_pr = Window.partitionBy("operator").orderBy("sku_weight_kg", "id").rowsBetween(-3, -1)
w_warehouse_city_warehouse_id = Window.partitionBy("warehouse_city", "warehouse_id")
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_warehouse_id_by_sku_weight_k_3 = Window.partitionBy("warehouse_id").orderBy("sku_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
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

joined = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.category").alias("sku_category"),
    )
)

windowed = (
    joined
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        "warehouse_city",
        "sku_weight_kg",
        F.size(F.collect_set("sku_category").over(w_move_type)).alias("distinct_sku_category_in_move_type"),
        F.round(F.avg("unit_price").over(w_all_by_qty_moving_3), 2).alias("unit_price_avg_4"),
        F.ntile(4).over(w_all_by_qty).alias("qty_quartile"),
        F.sum("sku_weight_kg").over(w_operator_by_sku_weight_kg_ru).alias("running_sku_weight_kg"),
        F.ntile(5).over(w_operator_by_sku_weight_kg).alias("sku_weight_kg_quintile"),
        F.sum("unit_price").over(w_move_type).alias("move_type_sum_unit_price"),
        F.round(F.avg("sku_weight_kg").over(w_warehouse_id_by_sku_weight_k), 2).alias("sku_weight_kg_avg_1_prior"),
        F.ntile(5).over(w_operator_warehouse_id_by_mov).alias("moved_at_quintile"),
        F.nth_value("sku_weight_kg", 2).over(w_sku_id_by_unit_price_full).alias("second_sku_weight_kg"),
        F.dense_rank().over(w_warehouse_id_by_sku_weight_k_2).alias("sku_weight_kg_dense_rank"),
        F.round(F.avg("qty").over(w_operator_by_sku_weight_kg_pr), 2).alias("qty_avg_3_prior"),
        F.min("sku_weight_kg").over(w_warehouse_city_warehouse_id).alias("warehouse_city_warehouse_id_min_sku_weight_kg"),
        F.row_number().over(w_sku_id_by_unit_price).alias("unit_price_seq"),
        F.nth_value("sku_weight_kg", 2).over(w_warehouse_id_by_sku_weight_k_3).alias("second_sku_weight_kg_2"),
        F.round(F.cume_dist().over(w_operator_warehouse_id_by_mov), 3).alias("moved_at_cume_dist"),
    )
)

result = (
    windowed
    .filter(F.col("sku_id") >= 11)
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "sku_weight_kg",
        "distinct_sku_category_in_move_type",
        "unit_price_avg_4",
        "qty_quartile",
        "running_sku_weight_kg",
        "sku_weight_kg_quintile",
        "move_type_sum_unit_price",
        "sku_weight_kg_avg_1_prior",
        "moved_at_quintile",
        "second_sku_weight_kg",
        "sku_weight_kg_dense_rank",
        "qty_avg_3_prior",
        "warehouse_city_warehouse_id_min_sku_weight_kg",
        "unit_price_seq",
        "moved_at_cume_dist",
        F.lower(F.regexp_replace(F.trim(F.col("warehouse_city")), " ", "_")).alias("warehouse_city_slug"),
        F.substring(F.trim(F.col("warehouse_city")), 1, 2).alias("warehouse_city_prefix"),
    )
)
