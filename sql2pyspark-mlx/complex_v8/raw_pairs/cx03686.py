from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_operator_sku_id_by_qty_prior = Window.partitionBy("operator", "sku_id").orderBy("qty", "id").rowsBetween(-2, -1)
w_move_type_by_qty_ties = Window.partitionBy("move_type").orderBy("qty")
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_move_type_by_qty = Window.partitionBy("move_type").orderBy("qty", "id")
w_warehouse_id_sku_id_by_qty_f = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy("unit_price", "id")
w_operator_sku_id_by_qty = Window.partitionBy("operator", "sku_id").orderBy("qty", "id")
w_move_type_by_qty_prior_1 = Window.partitionBy("move_type").orderBy("qty", "id").rowsBetween(-1, -1)
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_warehouse_id_by_unit_price_r = Window.partitionBy("warehouse_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_move_type_outbound = Window.orderBy(F.col("n_move_type_outbound").desc(), "qty_dense_rank", "warehouse_id", "qty_dense_rank_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_move_type_outbound_2 = Window.orderBy(F.col("n_move_type_outbound").desc())

filtered_moves = (
    df_stock_moves
    .filter(F.col("operator") == "op_eve")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

sequenced = (
    filtered_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
        F.max("qty").over(w_operator_sku_id_by_qty_prior).alias("qty_max_2_prior"),
        F.dense_rank().over(w_move_type_by_qty_ties).alias("qty_dense_rank"),
        F.size(F.collect_set("sku_id").over(w_move_type_operator)).alias("distinct_sku_id_in_move_type_operator"),
        (F.col("qty") - F.lag("qty").over(w_move_type_by_qty)).alias("qty_change"),
        F.last("unit_price").over(w_warehouse_id_sku_id_by_qty_f).alias("last_unit_price"),
        F.rank().over(w_warehouse_id_by_unit_price).alias("unit_price_rank"),
        (F.col("unit_price") - F.lag("unit_price").over(w_operator_sku_id_by_qty)).alias("unit_price_change"),
        F.sum("unit_price").over(w_move_type_by_qty_prior_1).alias("unit_price_sum_1_prior"),
        F.ntile(3).over(w_operator_sku_id_by_qty).alias("qty_tercile"),
        F.dense_rank().over(w_all_by_qty).alias("qty_dense_rank_2"),
        F.first("qty", ignorenulls=True).over(w_warehouse_id_by_unit_price_r).alias("first_known_qty"),
        F.row_number().over(w_operator_sku_id_by_qty).alias("qty_seq"),
        F.round(F.cume_dist().over(w_move_type_by_qty_ties), 3).alias("qty_cume_dist"),
    )
)

first_rows = (
    sequenced
    .filter((F.col("unit_price_rank") <= 2) & (F.col("sku_id").isNotNull()))
    .drop("unit_price_rank")
)

grouped = (
    first_rows
    .groupBy("qty_dense_rank", "warehouse_id", "qty_dense_rank_2")
    .agg(
        F.countDistinct("operator").alias("distinct_operators"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.count(F.when(F.col("move_type") == "OUTBOUND", 1)).alias("n_move_type_outbound"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
)

windowed = (
    grouped
    .select(
        "qty_dense_rank",
        "warehouse_id",
        "qty_dense_rank_2",
        "n_move_type_outbound",
        "operator_list",
        F.sum("distinct_operators").over(w_all_by_n_move_type_outbound).alias("running_distinct_operators"),
        F.rank().over(w_all_by_n_move_type_outbound_2).alias("n_move_type_outbound_rank"),
        F.round(F.cume_dist().over(w_all_by_n_move_type_outbound_2), 3).alias("n_move_type_outbound_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "qty_dense_rank",
        "warehouse_id",
        "qty_dense_rank_2",
        "n_move_type_outbound",
        "operator_list",
        "running_distinct_operators",
        "n_move_type_outbound_cume_dist",
        F.coalesce(F.col("qty_dense_rank_2"), F.lit(0)).alias("qty_dense_rank_2_filled"),
        F.round(F.col("qty_dense_rank") / F.nullif(F.col("n_move_type_outbound"), F.lit(0)), 3).alias("qty_dense_rank_per_n_move_type_outbound"),
    )
    .orderBy("qty_dense_rank_2_filled")
)
