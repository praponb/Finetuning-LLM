from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_city = Window.partitionBy("sku_id", "warehouse_city")
w_warehouse_city_sku_id_by_uni = Window.partitionBy("warehouse_city", "sku_id").orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w_warehouse_city_sku_id_by_uni_2 = Window.partitionBy("warehouse_city", "sku_id").orderBy("unit_price", "id").rowsBetween(-1, -1)
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc_nulls_first(), "id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_warehouse_city_operator_by_u = Window.partitionBy("warehouse_city", "operator").orderBy("unit_price", "id")
w_operator_sku_id_by_unit_pric = Window.partitionBy("operator", "sku_id").orderBy("unit_price", "id")
w_warehouse_city_sku_id_by_uni_3 = Window.partitionBy("warehouse_city", "sku_id").orderBy("unit_price", "id")
w_operator_operator_or_unknown = Window.partitionBy("operator", "operator_or_unknown").orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_sku_id_by_unit_pric_2 = Window.partitionBy("operator", "sku_id").orderBy("unit_price")
w_all_by_unit_price_prior_1 = Window.orderBy("unit_price", "id").rowsBetween(-1, -1)
w_warehouse_city_warehouse_id = Window.partitionBy("warehouse_city", "warehouse_id")
w_all_by_unit_price_2 = Window.orderBy("unit_price", "id")
w_warehouse_city_sku_id_by_uni_4 = Window.partitionBy("warehouse_city", "sku_id").orderBy("unit_price", "id").rowsBetween(-3, -1)
w_move_type = Window.partitionBy("move_type")
w_unit_price_seq_by_prev_unit = Window.partitionBy("unit_price_seq").orderBy(F.col("prev_unit_price").desc(), "id").rowsBetween(-2, -1)
w_operator_or_unknown_unit_pri = Window.partitionBy("operator_or_unknown", "unit_price_decile").orderBy(F.col("unit_price_decile").desc(), "id")
w_unit_price_seq_by_prev_unit_2 = Window.partitionBy("unit_price_seq").orderBy(F.col("prev_unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_unit_price_decile_by = Window.partitionBy("n_rows", "unit_price_decile").orderBy("n_rows", "unit_price_decile")
w_n_rows_unit_price_decile_by_2 = Window.partitionBy("n_rows", "unit_price_decile").orderBy("n_rows")

base = (
    df_stock_moves
    .filter((F.col("sku_id") <= 8) | (F.col("unit_price") < 330.9))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.1, 2).alias("unit_price_adj"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

enriched_warehouse = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.operator_or_unknown",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

metrics = (
    enriched_warehouse
    .select(
        "*",
        F.sum("unit_price").over(w_sku_id_warehouse_city).alias("sku_id_warehouse_city_sum_unit_price"),
        F.round(F.avg("unit_price").over(w_warehouse_city_sku_id_by_uni), 2).alias("unit_price_avg_4"),
        F.sum("unit_price").over(w_warehouse_city_sku_id_by_uni_2).alias("unit_price_sum_1_prior"),
        F.ntile(10).over(w_all_by_unit_price).alias("unit_price_decile"),
        F.min("unit_price").over(w_sku_id_operator).alias("sku_id_operator_min_unit_price"),
        F.lag("unit_price", 3).over(w_warehouse_city_operator_by_u).alias("prev3_unit_price"),
        F.lead("unit_price", 1, 0).over(w_operator_sku_id_by_unit_pric).alias("next_unit_price"),
        F.round(F.avg("unit_price").over(w_sku_id_operator), 2).alias("sku_id_operator_avg_unit_price"),
        F.row_number().over(w_warehouse_city_sku_id_by_uni_3).alias("rn"),
        F.sum("unit_price").over(w_operator_operator_or_unknown).alias("running_unit_price"),
        F.row_number().over(w_all_by_unit_price).alias("unit_price_seq"),
        F.dense_rank().over(w_operator_sku_id_by_unit_pric_2).alias("unit_price_dense_rank"),
        F.sum("unit_price").over(w_all_by_unit_price_prior_1).alias("unit_price_sum_1_prior_2"),
        F.size(F.collect_set("sku_id").over(w_warehouse_city_warehouse_id)).alias("distinct_sku_id_in_warehouse_city_warehouse_id"),
        F.lag("unit_price").over(w_all_by_unit_price_2).alias("prev_unit_price"),
        F.round(F.avg("unit_price").over(w_warehouse_city_sku_id_by_uni_4), 2).alias("unit_price_avg_3_prior"),
    )
)

top_rows = (
    metrics
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_trends = (
    top_rows
    .select(
        "*",
        F.size(F.collect_set("warehouse_id").over(w_move_type)).alias("distinct_warehouse_id_in_move_type"),
        F.sum("unit_price_sum_1_prior_2").over(w_unit_price_seq_by_prev_unit).alias("unit_price_sum_1_prior_2_sum_2_prior"),
        F.row_number().over(w_operator_or_unknown_unit_pri).alias("rn"),
        F.first("prev_unit_price").over(w_unit_price_seq_by_prev_unit_2).alias("first_prev_unit_price"),
    )
)

grouped = (
    with_trends
    .filter(F.col("prev_unit_price") <= 396.4)
    .groupBy("unit_price_decile")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("distinct_warehouse_id_in_move_type").alias("total_distinct_warehouse_id_in_move_type"),
    )
)

with_windows = (
    grouped
    .select(
        "unit_price_decile",
        "n_rows",
        "total_distinct_warehouse_id_in_move_type",
        F.lag("unit_price_decile").over(w_n_rows_unit_price_decile_by).alias("prev_unit_price_decile"),
        F.round(F.percent_rank().over(w_n_rows_unit_price_decile_by), 3).alias("n_rows_percent_rank"),
        F.round(F.cume_dist().over(w_n_rows_unit_price_decile_by_2), 3).alias("n_rows_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "unit_price_decile",
        "n_rows",
        "total_distinct_warehouse_id_in_move_type",
        "n_rows_percent_rank",
        "n_rows_cume_dist",
    )
    .orderBy("n_rows_cume_dist", F.col("unit_price_decile").desc_nulls_first())
)
