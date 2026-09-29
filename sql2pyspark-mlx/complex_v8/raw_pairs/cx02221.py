from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_running = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_prior_1 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-1, -1)
w_operator = Window.partitionBy("operator")
w_operator_by_qty_moving_2 = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id").rowsBetween(-2, Window.currentRow)
w_operator_warehouse_id_by_uni = Window.partitionBy("operator", "warehouse_id").orderBy("unit_price", "id")
w_operator_warehouse_id_by_uni_2 = Window.partitionBy("operator", "warehouse_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_warehouse_id_by_uni_3 = Window.partitionBy("operator", "warehouse_id").orderBy("unit_price")
w_operator_warehouse_id_by_uni_4 = Window.partitionBy("operator", "warehouse_id").orderBy("unit_price", "id").rowsBetween(-3, -1)

recent_moves = (
    df_stock_moves
    .filter(F.col("move_type").isin("TRANSFER", "INBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

sequenced = (
    recent_moves
    .select(
        "*",
        F.count(F.lit(1)).over(w_all_by_qty_running).alias("running_count"),
        F.sum("qty").over(w_all_by_qty_prior_1).alias("qty_sum_1_prior"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator), F.lit(0)), 4).alias("unit_price_share_of_operator"),
        F.round(F.avg("unit_price").over(w_operator_by_qty_moving_2), 2).alias("unit_price_avg_3"),
        F.row_number().over(w_operator_warehouse_id_by_uni).alias("rn"),
        F.sum("qty").over(w_operator).alias("operator_sum_qty"),
        F.first("qty").over(w_operator_warehouse_id_by_uni_2).alias("first_qty"),
        F.round(F.percent_rank().over(w_operator_warehouse_id_by_uni_3), 3).alias("unit_price_percent_rank"),
        F.max("qty").over(w_operator_warehouse_id_by_uni_4).alias("qty_max_3_prior"),
    )
)

result = (
    sequenced
    .filter(F.col("qty_max_3_prior") >= 601)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "running_count",
        "unit_price_share_of_operator",
        "unit_price_avg_3",
        "rn",
        "unit_price_percent_rank",
        "qty_max_3_prior",
        F.when(F.col("operator").isin("op_chan", "op_dew"), "group_a").when(F.col("operator") == "op_ann", "group_b").otherwise("other").alias("operator_group"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
    .orderBy("id")
    .limit(5)
)
