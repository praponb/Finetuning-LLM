from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_warehouse_id_moved_at_month = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy("unit_price_filled", "id").rowsBetween(-6, -1)
w_all_by_qty_ties = Window.orderBy("qty")
w_operator_by_unit_price_fille = Window.partitionBy("operator").orderBy(F.col("unit_price_filled").asc_nulls_last())
w_warehouse_id_moved_at_month_2 = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy("unit_price_filled")
w_operator_by_unit_price_fille_2 = Window.partitionBy("operator").orderBy(F.col("unit_price_filled").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_operator = Window.partitionBy("operator")
w_unit_price_filled_dense_rank = Window.partitionBy("unit_price_filled_dense_rank", "distinct_warehouse_id_in_operator_sku_id").orderBy(F.col("moved_at").desc(), "id")
w_all_by_rows_in_operator_sku = Window.orderBy(F.col("rows_in_operator_sku_id").desc(), "id")

cleaned_moves = (
    df_stock_moves
    .filter((F.col("qty") >= 98) | (F.col("operator") == "op_chan"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

combined_details = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_filled",
        "sk.sku_code",
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.max("qty").over(w_operator_sku_id).alias("operator_sku_id_max_qty"),
        F.round(F.avg("unit_price_filled").over(w_warehouse_id_moved_at_month), 2).alias("unit_price_filled_avg_6_prior"),
        F.size(F.collect_set("warehouse_id").over(w_operator_sku_id)).alias("distinct_warehouse_id_in_operator_sku_id"),
        F.round(F.cume_dist().over(w_all_by_qty_ties), 3).alias("qty_cume_dist"),
        F.sum("unit_price_filled").over(w_operator_sku_id).alias("operator_sku_id_sum_unit_price_filled"),
        F.rank().over(w_operator_by_unit_price_fille).alias("unit_price_filled_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_operator_sku_id), 2).alias("unit_price_vs_operator_sku_id_avg"),
        F.dense_rank().over(w_warehouse_id_moved_at_month_2).alias("unit_price_filled_dense_rank"),
        F.max("unit_price_filled").over(w_operator_by_unit_price_fille_2).alias("unit_price_filled_max_7"),
        F.count(F.lit(1)).over(w_operator_sku_id).alias("rows_in_operator_sku_id"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "operator",
        "unit_price_filled",
        "sku_code",
        "operator_sku_id_max_qty",
        "unit_price_filled_avg_6_prior",
        "distinct_warehouse_id_in_operator_sku_id",
        "qty_cume_dist",
        "operator_sku_id_sum_unit_price_filled",
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
        F.dense_rank().over(w_unit_price_filled_dense_rank).alias("moved_at_dense_rank"),
        F.round(F.col("operator_sku_id_max_qty") - F.avg("operator_sku_id_max_qty").over(w_operator), 2).alias("operator_sku_id_max_qty_vs_operator_avg"),
        F.dense_rank().over(w_all_by_rows_in_operator_sku).alias("rows_in_operator_sku_id_dense_rank"),
    )
)

stats = (
    ranked_2
    .groupBy("moved_at_dense_rank", "operator")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("qty_cume_dist") / F.nullif(F.sum("rows_in_operator"), F.lit(0)), 3).alias("qty_cume_dist_to_rows_in_operator_ratio"),
        F.sum(F.when(F.col("operator") == "op_dew", F.col("distinct_warehouse_id_in_operator_sku_id")).otherwise(0)).alias("distinct_warehouse_id_in_operator_sku_id_operato"),
        F.sum(F.when(F.col("operator").isin("op_eve", "op_bee"), 1).otherwise(0)).alias("n_operator_in_op_eve_op_bee"),
        F.round(F.sum("moved_at_dense_rank") / F.nullif(F.sum("operator_sku_id_max_qty"), F.lit(0)), 3).alias("moved_at_dense_rank_to_operator_sku_id_max_qty_r"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
)

result = (
    stats
    .filter(F.col("moved_at_dense_rank_to_operator_sku_id_max_qty_r") > 2.0)
    .select(
        "moved_at_dense_rank",
        "operator",
        "row_count",
        "distinct_warehouse_id_in_operator_sku_id_operato",
        "moved_at_dense_rank_to_operator_sku_id_max_qty_r",
        "operator_list",
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean"),
        F.abs(F.col("moved_at_dense_rank") - 6).alias("moved_at_dense_rank_distance"),
    )
    .orderBy("moved_at_dense_rank", F.col("row_count").desc())
)
