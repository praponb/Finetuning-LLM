from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())
w_all_by_qty_full = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_qty_running = Window.partitionBy("operator").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_move_type_by_unit_price_ties = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc())
w_operator_by_qty = Window.partitionBy("operator").orderBy("qty", "id")
w_operator_move_type_or_unknow = Window.partitionBy("operator", "move_type_or_unknown").orderBy(F.col("qty").asc_nulls_last(), "id")
w_move_type_by_unit_price = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), "id")
w_operator_move_type_or_unknow_2 = Window.partitionBy("operator", "move_type_or_unknown").orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_move_type_or_unknown = Window.partitionBy("sku_id", "move_type_or_unknown")
w_operator_by_qty_moving_2 = Window.partitionBy("operator").orderBy("qty", "id").rowsBetween(-2, Window.currentRow)
w_all_by_unit_price_max_3_ties = Window.orderBy(F.col("unit_price_max_3").desc())
w_all_by_unit_price_rank = Window.orderBy("unit_price_rank", "id")
w_unit_price_rank_move_type = Window.partitionBy("unit_price_rank", "move_type")
w_warehouse_id_move_type_or_un = Window.partitionBy("warehouse_id", "move_type_or_unknown")
w_all_by_sku_id_move_type_max = Window.orderBy(F.col("sku_id_move_type_max_qty").desc(), "id")
w_all_by_unit_price_rank_movin = Window.orderBy("unit_price_rank", "id").rowsBetween(-1, Window.currentRow)

scoped = (
    df_stock_moves
    .filter((F.col("operator") != "op_ann") & (F.col("move_type").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

with_details_sku = (
    scoped.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.move_type_or_unknown",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

ranked = (
    with_details_sku
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "unit_price",
        "move_type_or_unknown",
        "sku_unit_cost",
        F.rank().over(w_all_by_qty_ties).alias("qty_rank"),
        F.first("sku_unit_cost").over(w_all_by_qty_full).alias("first_sku_unit_cost"),
        F.sum("qty").over(w_operator_by_qty_running).alias("running_qty"),
        F.max("qty").over(w_sku_id_move_type).alias("sku_id_move_type_max_qty"),
        F.min("unit_price").over(w_sku_id_move_type).alias("sku_id_move_type_min_unit_price"),
        F.round(F.percent_rank().over(w_move_type_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
        F.lead("unit_price", 3).over(w_operator_by_qty).alias("next3_unit_price"),
        F.rank().over(w_move_type_by_unit_price_ties).alias("unit_price_rank"),
        F.round(F.percent_rank().over(w_operator_move_type_or_unknow), 3).alias("qty_percent_rank"),
        F.lead("unit_price").over(w_move_type_by_unit_price).alias("next_unit_price"),
        F.max("unit_price").over(w_operator_move_type_or_unknow_2).alias("max_unit_price_so_far"),
        F.count(F.lit(1)).over(w_sku_id_move_type).alias("rows_in_sku_id_move_type"),
        F.count(F.lit(1)).over(w_sku_id_move_type_or_unknown).alias("rows_in_sku_id_move_type_or_unknown"),
        F.max("unit_price").over(w_operator_by_qty_moving_2).alias("unit_price_max_3"),
        F.lag("qty").over(w_operator_by_qty).alias("prev_qty"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_move_type), F.lit(0)), 4).alias("unit_price_share_of_sku_id_move_type"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "unit_price",
        "move_type_or_unknown",
        "qty_rank",
        "first_sku_unit_cost",
        "sku_id_move_type_max_qty",
        "sku_id_move_type_min_unit_price",
        "next3_unit_price",
        "qty_percent_rank",
        "next_unit_price",
        "rows_in_sku_id_move_type",
        "rows_in_sku_id_move_type_or_unknown",
        "prev_qty",
        "unit_price_share_of_sku_id_move_type",
        F.rank().over(w_all_by_unit_price_max_3_ties).alias("unit_price_max_3_rank"),
        F.row_number().over(w_all_by_unit_price_rank).alias("unit_price_rank_seq"),
        F.round(F.col("qty_percent_rank") / F.nullif(F.sum("qty_percent_rank").over(w_unit_price_rank_move_type), F.lit(0)), 4).alias("qty_percent_rank_share_of_unit_price_rank_move_t"),
        F.round(F.col("qty_percent_rank") - F.avg("qty_percent_rank").over(w_warehouse_id_move_type_or_un), 2).alias("qty_percent_rank_vs_warehouse_id_move_type_or_un"),
        F.lead("max_unit_price_so_far").over(w_all_by_sku_id_move_type_max).alias("next_max_unit_price_so_far"),
        F.max("unit_price").over(w_all_by_unit_price_rank_movin).alias("unit_price_max_2"),
        F.ntile(10).over(w_all_by_sku_id_move_type_max).alias("sku_id_move_type_max_qty_decile"),
        F.round(F.col("max_unit_price_so_far") - F.avg("max_unit_price_so_far").over(w_unit_price_rank_move_type), 2).alias("max_unit_price_so_far_vs_unit_price_rank_move_ty"),
    )
)

result = (
    windowed
    .filter(F.col("next_unit_price").between(112.8, 313.8))
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "unit_price",
        "move_type_or_unknown",
        "first_sku_unit_cost",
        "sku_id_move_type_min_unit_price",
        "next3_unit_price",
        "qty_percent_rank",
        "next_unit_price",
        "rows_in_sku_id_move_type",
        "prev_qty",
        "unit_price_max_3_rank",
        "unit_price_rank_seq",
        "qty_percent_rank_share_of_unit_price_rank_move_t",
        "qty_percent_rank_vs_warehouse_id_move_type_or_un",
        "unit_price_max_2",
        "sku_id_move_type_max_qty_decile",
        "max_unit_price_so_far_vs_unit_price_rank_move_ty",
    )
)
