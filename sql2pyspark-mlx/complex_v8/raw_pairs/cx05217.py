from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id_by_un = Window.partitionBy("move_type", "warehouse_id").orderBy("unit_price", "id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_all_by_unit_price_vs_sku_id = Window.orderBy(F.col("unit_price_vs_sku_id_operator_avg").desc(), "id")
w_operator_by_unit_price_dense = Window.partitionBy("operator").orderBy(F.col("unit_price_dense_rank").desc(), "id").rowsBetween(-1, Window.currentRow)
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_all_by_unit_price_vs_sku_id_2 = Window.orderBy(F.col("unit_price_vs_sku_id_operator_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_moves = (
    df_stock_moves
    .filter(F.col("qty") < 300)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
    )
)

metrics = (
    cleaned_moves
    .select(
        "*",
        F.dense_rank().over(w_move_type_warehouse_id_by_un).alias("unit_price_dense_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_operator), 2).alias("unit_price_vs_sku_id_operator_avg"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator",
        "unit_price",
        "unit_price_dense_rank",
        "unit_price_vs_sku_id_operator_avg",
        (F.col("unit_price_dense_rank") - F.lag("unit_price_dense_rank").over(w_all_by_unit_price_vs_sku_id)).alias("unit_price_dense_rank_change"),
        F.max("unit_price_dense_rank").over(w_operator_by_unit_price_dense).alias("unit_price_dense_rank_max_2"),
        F.ntile(5).over(w_all_by_unit_price_vs_sku_id).alias("unit_price_vs_sku_id_operator_avg_quintile"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id_move_type), F.lit(0)), 4).alias("unit_price_share_of_warehouse_id_move_type"),
        F.size(F.collect_set("operator").over(w_warehouse_id_move_type)).alias("distinct_operator_in_warehouse_id_move_type"),
        F.nth_value("unit_price", 2).over(w_all_by_unit_price_vs_sku_id_2).alias("second_unit_price"),
    )
)

result = (
    windowed
    .cube("unit_price_dense_rank", "distinct_operator_in_warehouse_id_move_type", "move_type")
    .agg(
        F.grouping("unit_price_dense_rank").alias("is_total_unit_price_dense_rank"),
        F.count(F.when(F.col("operator") == "op_bee", 1)).alias("n_operator_op_bee"),
    )
    .select(
        "unit_price_dense_rank",
        "distinct_operator_in_warehouse_id_move_type",
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "is_total_unit_price_dense_rank",
        "n_operator_op_bee",
    )
    .orderBy(
        F.col("unit_price_dense_rank").asc_nulls_last(),
        F.col("distinct_operator_in_warehouse_id_move_type").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
