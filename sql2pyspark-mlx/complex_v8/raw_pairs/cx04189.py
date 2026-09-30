from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_move_type_by_unit_p = Window.partitionBy("operator", "move_type").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-6, Window.currentRow)
w_warehouse_id_move_type_by_un = Window.partitionBy("warehouse_id", "move_type").orderBy("unit_price", "id")
w_move_type = Window.partitionBy("move_type")
w_move_type_by_qty_moving_2 = Window.partitionBy("move_type").orderBy("qty", "id").rowsBetween(-2, Window.currentRow)
w_operator_move_type_by_unit_p_2 = Window.partitionBy("operator", "move_type").orderBy(F.col("unit_price").desc(), "id")
w_move_type_by_qty = Window.partitionBy("move_type").orderBy("qty", "id")
w_warehouse_id_move_type_by_un_2 = Window.partitionBy("warehouse_id", "move_type").orderBy("unit_price")
w_distinct_operator_in_move_ty = Window.partitionBy("distinct_operator_in_move_type")
w_all_by_cnt = Window.orderBy("cnt", "operator", "sku_id", "distinct_operator_in_move_type")

filtered = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")) & (F.col("sku_id").isNotNull()))
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

with_windows = (
    filtered
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "unit_price",
        F.sum("unit_price").over(w_operator_move_type_by_unit_p).alias("unit_price_sum_7"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_warehouse_id_move_type_by_un))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.sum("unit_price").over(w_move_type_by_qty_moving_2).alias("unit_price_sum_3"),
        (F.col("unit_price") - F.lag("unit_price").over(w_operator_move_type_by_unit_p_2)).alias("unit_price_change"),
        F.ntile(4).over(w_warehouse_id_move_type_by_un).alias("unit_price_quartile"),
        F.size(F.collect_set("warehouse_id").over(w_move_type)).alias("distinct_warehouse_id_in_move_type"),
        F.max("qty").over(w_move_type).alias("move_type_max_qty"),
        F.row_number().over(w_move_type_by_qty).alias("rn"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type), F.lit(0)), 4).alias("unit_price_share_of_move_type"),
        F.size(F.collect_set("operator").over(w_move_type)).alias("distinct_operator_in_move_type"),
        F.ntile(4).over(w_move_type_by_qty).alias("qty_quartile"),
        F.rank().over(w_warehouse_id_move_type_by_un_2).alias("unit_price_rank"),
    )
)

grouped = (
    with_windows
    .groupBy("operator", "sku_id", "distinct_operator_in_move_type")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("unit_price_rank") / F.nullif(F.sum("unit_price_change"), F.lit(0)), 3).alias("unit_price_rank_to_unit_price_change_ratio"),
    )
    .filter(F.col("cnt") >= 2)
)

windowed = (
    grouped
    .select(
        "*",
        F.size(F.collect_set("operator").over(w_distinct_operator_in_move_ty)).alias("distinct_operator_in_distinct_operator_in_move_t"),
        F.row_number().over(w_all_by_cnt).alias("seq_no"),
        F.round(F.col("cnt") / F.nullif(F.sum("cnt").over(w_distinct_operator_in_move_ty), F.lit(0)), 4).alias("cnt_share_of_distinct_operator_in_move_type"),
    )
)

result = (
    windowed
    .select(
        "operator",
        "sku_id",
        "distinct_operator_in_move_type",
        "cnt",
        "unit_price_rank_to_unit_price_change_ratio",
        "distinct_operator_in_distinct_operator_in_move_t",
        "seq_no",
        "cnt_share_of_distinct_operator_in_move_type",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
    .orderBy(F.col("sku_id").desc_nulls_first(), F.col("seq_no").desc())
)
