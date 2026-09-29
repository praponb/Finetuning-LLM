from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_moved_at_to_date = Window.partitionBy("sku_id", F.to_date("moved_at"))
w_warehouse_id_move_type_by_op = Window.partitionBy("warehouse_id", "move_type").orderBy("operator_len", "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_moved_at_month_operator_len = Window.partitionBy(F.date_trunc("month", "moved_at"), "operator_len")
w_warehouse_id_by_unit_price_t = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc_nulls_first())
w_sku_id_by_unit_price_prior_3 = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(-3, -1)
w_unit_price_rank_prev3_moved = Window.partitionBy("unit_price_rank", F.to_date("prev3_moved_at"))
w_unit_price_rank = Window.partitionBy("unit_price_rank")

valid = (
    df_stock_moves
    .filter((F.col("operator") == "op_bee") & (F.col("warehouse_id") <= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

sequenced = (
    valid
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "operator_len",
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_id_moved_at_to_date), F.lit(0)), 4).alias("qty_share_of_sku_id_moved_at_to_date"),
        F.lag("moved_at", 3).over(w_warehouse_id_move_type_by_op).alias("prev3_moved_at"),
        F.max("operator_len").over(w_sku_id_moved_at_to_date).alias("sku_id_moved_at_to_date_max_operator_len"),
        F.round(F.avg("qty").over(w_warehouse_id), 2).alias("warehouse_id_avg_qty"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_moved_at_month_operator_len), F.lit(0)), 4).alias("qty_share_of_moved_at_month_operator_len"),
        F.min("operator_len").over(w_sku_id_moved_at_to_date).alias("sku_id_moved_at_to_date_min_operator_len"),
        F.rank().over(w_warehouse_id_by_unit_price_t).alias("unit_price_rank"),
        F.round(F.col("operator_len") - F.avg("operator_len").over(w_warehouse_id), 2).alias("operator_len_vs_warehouse_id_avg"),
        F.round(F.avg("unit_price").over(w_sku_id_by_unit_price_prior_3), 2).alias("unit_price_avg_3_prior"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_moved_at_to_date), F.lit(0)), 4).alias("unit_price_share_of_sku_id_moved_at_to_date"),
        F.round(F.percent_rank().over(w_warehouse_id_by_unit_price_t), 3).alias("unit_price_percent_rank"),
    )
)

top_rows = sequenced.filter(F.col("unit_price_rank") <= 5)

ranked = (
    top_rows
    .select(
        "*",
        F.round(F.col("unit_price_rank") - F.avg("unit_price_rank").over(w_unit_price_rank_prev3_moved), 2).alias("unit_price_rank_vs_unit_price_rank_prev3_moved_a"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_unit_price_rank), F.lit(0)), 4).alias("unit_price_share_of_unit_price_rank"),
        F.count(F.lit(1)).over(w_unit_price_rank_prev3_moved).alias("rows_in_unit_price_rank_prev3_moved_at_to_date"),
        F.size(F.collect_set("move_type").over(w_unit_price_rank_prev3_moved)).alias("distinct_move_type_in_unit_price_rank_prev3_move"),
    )
)

result = (
    ranked
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "operator_len",
        "qty_share_of_sku_id_moved_at_to_date",
        "sku_id_moved_at_to_date_max_operator_len",
        "warehouse_id_avg_qty",
        "qty_share_of_moved_at_month_operator_len",
        "unit_price_rank",
        "unit_price_share_of_sku_id_moved_at_to_date",
        "unit_price_percent_rank",
        "unit_price_rank_vs_unit_price_rank_prev3_moved_a",
        "unit_price_share_of_unit_price_rank",
        "rows_in_unit_price_rank_prev3_moved_at_to_date",
        "distinct_move_type_in_unit_price_rank_prev3_move",
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
    )
    .orderBy(
        F.col("id").desc_nulls_first(),
        "sku_id_moved_at_to_date_max_operator_len",
    )
    .limit(10)
)
