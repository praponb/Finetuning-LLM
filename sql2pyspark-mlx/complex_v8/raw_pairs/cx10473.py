from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_warehouse_id_by_qty_plus_uni = Window.partitionBy("warehouse_id").orderBy(F.col("qty_plus_unit_price").desc_nulls_first(), "id")
w_all_by_qty_plus_unit_price = Window.orderBy("qty_plus_unit_price", "id")
w_warehouse_id_by_qty_plus_uni_2 = Window.partitionBy("warehouse_id").orderBy(F.col("qty_plus_unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_qty_plus_unit_p = Window.partitionBy("move_type").orderBy(F.col("qty_plus_unit_price").desc(), "id").rowsBetween(-2, -1)
w_move_type_by_qty_plus_unit_p_2 = Window.partitionBy("move_type").orderBy(F.col("qty_plus_unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_operator_not_in_op_bee_op = Window.partitionBy("n_operator_not_in_op_bee_op_de", "distinct_sku_ids").orderBy("total_rows_in_operator_sku_id", "move_type", "qty_plus_unit_price_dense_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_qty_plus_unit_price_dense_ra = Window.partitionBy("qty_plus_unit_price_dense_rank", "n_rows").orderBy(F.col("n_operator_not_in_op_bee_op_de").desc(), "move_type", "qty_plus_unit_price_dense_rank").rowsBetween(-3, Window.currentRow)
w_qty_plus_unit_price_dense_ra_2 = Window.partitionBy("qty_plus_unit_price_dense_rank", "n_rows").orderBy(F.col("n_operator_not_in_op_bee_op_de").desc())

prepared_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

with_parent_sku = (
    prepared_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.unit_cost") >= 60.9), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.qty_plus_unit_price",
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

windowed = (
    with_parent_sku
    .select(
        "*",
        F.round(F.col("qty_plus_unit_price") / F.nullif(F.sum("qty_plus_unit_price").over(w_operator_sku_id), F.lit(0)), 4).alias("qty_plus_unit_price_share_of_operator_sku_id"),
        (F.col("qty_plus_unit_price") - F.lag("qty_plus_unit_price").over(w_warehouse_id_by_qty_plus_uni)).alias("qty_plus_unit_price_change"),
        F.round(F.percent_rank().over(w_all_by_qty_plus_unit_price), 3).alias("qty_plus_unit_price_percent_rank"),
        F.lead("qty_plus_unit_price", 2).over(w_all_by_qty_plus_unit_price).alias("next2_qty_plus_unit_price"),
        F.round(F.avg("qty_plus_unit_price").over(w_operator_sku_id), 2).alias("operator_sku_id_avg_qty_plus_unit_price"),
        F.min("qty_plus_unit_price").over(w_warehouse_id_by_qty_plus_uni_2).alias("min_qty_plus_unit_price_so_far"),
        F.round(F.avg("qty_plus_unit_price").over(w_move_type_by_qty_plus_unit_p), 2).alias("qty_plus_unit_price_avg_2_prior"),
        F.count(F.lit(1)).over(w_operator_sku_id).alias("rows_in_operator_sku_id"),
        F.dense_rank().over(w_warehouse_id_by_qty_plus_uni).alias("qty_plus_unit_price_dense_rank"),
        F.sum("qty_plus_unit_price").over(w_move_type_by_qty_plus_unit_p_2).alias("running_qty_plus_unit_price"),
    )
)

rollup_base = (
    windowed
    .groupBy("move_type", "qty_plus_unit_price_dense_rank")
    .agg(
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.sum(F.when(~F.col("operator").isin("op_bee", "op_dew"), 1).otherwise(0)).alias("n_operator_not_in_op_bee_op_de"),
        F.sum("rows_in_operator_sku_id").alias("total_rows_in_operator_sku_id"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

with_windows = (
    rollup_base
    .select(
        "*",
        F.last("distinct_sku_ids", ignorenulls=True).over(w_n_operator_not_in_op_bee_op).alias("last_known_distinct_sku_ids"),
        F.round(F.avg("distinct_sku_ids").over(w_qty_plus_unit_price_dense_ra), 2).alias("distinct_sku_ids_avg_4"),
        F.round(F.cume_dist().over(w_qty_plus_unit_price_dense_ra_2), 3).alias("n_operator_not_in_op_bee_op_de_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "move_type",
        "qty_plus_unit_price_dense_rank",
        "distinct_sku_ids",
        "n_operator_not_in_op_bee_op_de",
        "total_rows_in_operator_sku_id",
        "distinct_sku_ids_avg_4",
        "n_operator_not_in_op_bee_op_de_cume_dist",
        F.concat(F.col("move_type"), F.lit("#"), F.col("n_rows").cast("string")).alias("move_type_ref"),
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
    .orderBy(F.col("move_type").asc_nulls_last())
)
