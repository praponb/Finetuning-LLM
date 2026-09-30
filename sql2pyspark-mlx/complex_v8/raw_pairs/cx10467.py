from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_warehouse_id_by_uni = Window.partitionBy("operator", "warehouse_id").orderBy("unit_price", "id")
w_move_type = Window.partitionBy("move_type")
w_move_type_by_qty = Window.partitionBy("move_type").orderBy("qty", "id")
w_operator = Window.partitionBy("operator")
w_seq_no_by_move_type_max_unit = Window.partitionBy("seq_no").orderBy("move_type_max_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_seq_no_by_move_type_max_unit_2 = Window.partitionBy("seq_no").orderBy("move_type_max_unit_price", "id").rowsBetween(-1, -1)
w_warehouse_id_sku_id_by_unit = Window.partitionBy("warehouse_id", "sku_id").orderBy("unit_price_decile")
w_all_by_unit_price_decile_pri = Window.orderBy("unit_price_decile", "id").rowsBetween(-2, -1)
w_all_by_unit_price_decile = Window.orderBy("unit_price_decile", "id")
w_distinct_move_type_in_operat = Window.partitionBy("distinct_move_type_in_operator")

filtered_moves = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

with_trends = (
    filtered_moves
    .select(
        "*",
        F.ntile(10).over(w_operator_warehouse_id_by_uni).alias("unit_price_decile"),
        F.max("unit_price").over(w_move_type).alias("move_type_max_unit_price"),
        F.row_number().over(w_move_type_by_qty).alias("seq_no"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.min("unit_price").over(w_operator).alias("operator_min_unit_price"),
        F.size(F.collect_set("move_type").over(w_operator)).alias("distinct_move_type_in_operator"),
        F.first("unit_price").over(w_seq_no_by_move_type_max_unit).alias("first_unit_price"),
        F.round(F.avg("unit_price_decile").over(w_seq_no_by_move_type_max_unit_2), 2).alias("unit_price_decile_avg_1_prior"),
        F.rank().over(w_warehouse_id_sku_id_by_unit).alias("unit_price_decile_rank"),
        F.max("move_type_max_unit_price").over(w_operator).alias("operator_max_move_type_max_unit_price"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "operator_clean",
        "qty_plus_unit_price",
        "unit_price_decile",
        "move_type_max_unit_price",
        "seq_no",
        "unit_price_decile_avg_1_prior",
        F.round(F.avg("unit_price_decile").over(w_all_by_unit_price_decile_pri), 2).alias("unit_price_decile_avg_2_prior"),
        F.row_number().over(w_all_by_unit_price_decile).alias("unit_price_decile_seq"),
        F.round(F.avg("move_type_max_unit_price").over(w_all_by_unit_price_decile_pri), 2).alias("move_type_max_unit_price_avg_2_prior"),
        (F.col("seq_no") - F.lag("seq_no").over(w_all_by_unit_price_decile)).alias("seq_no_change"),
        F.count(F.lit(1)).over(w_distinct_move_type_in_operat).alias("rows_in_distinct_move_type_in_operator"),
    )
)

result = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "qty_plus_unit_price",
        "unit_price_decile",
        "move_type_max_unit_price",
        "seq_no",
        "unit_price_decile_avg_2_prior",
        "unit_price_decile_seq",
        "move_type_max_unit_price_avg_2_prior",
        "seq_no_change",
        "rows_in_distinct_move_type_in_operator",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
    .orderBy(F.col("unit_price_decile_avg_2_prior").desc_nulls_first(), "id")
    .limit(10)
)
