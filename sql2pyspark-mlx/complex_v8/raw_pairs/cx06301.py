from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_warehouse_id_by_qty_prior_1 = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(-1, -1)
w_warehouse_id_by_qty_ties = Window.partitionBy("warehouse_id").orderBy("qty")
w_all_by_qty_rank = Window.orderBy("qty_rank", "id")
w_all_by_qty_rank_prior_3 = Window.orderBy("qty_rank", "id").rowsBetween(-3, -1)
w_operator_or_unknown_distinct = Window.partitionBy("operator_or_unknown", "distinct_operator_or_unknown_in_sku_id_move_type")

scoped_moves = (
    df_stock_moves
    .filter(F.col("unit_price").between(37.6, 313.8))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

with_windows = (
    scoped_moves
    .select(
        "*",
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_move_type), 2).alias("unit_price_vs_sku_id_move_type_avg"),
        F.max("qty").over(w_warehouse_id_by_qty_prior_1).alias("qty_max_1_prior"),
        F.size(F.collect_set("operator_or_unknown").over(w_sku_id_move_type)).alias("distinct_operator_or_unknown_in_sku_id_move_type"),
        F.rank().over(w_warehouse_id_by_qty_ties).alias("qty_rank"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.lead("qty_max_1_prior", 2).over(w_all_by_qty_rank).alias("next2_qty_max_1_prior"),
        F.sum("qty_rank").over(w_all_by_qty_rank_prior_3).alias("qty_rank_sum_3_prior"),
        F.ntile(4).over(w_all_by_qty_rank).alias("qty_rank_quartile"),
        F.min("qty_rank").over(w_operator_or_unknown_distinct).alias("operator_or_unknown_distinct_operator_or_unknown"),
    )
)

without_matches = (
    with_trends.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator").isin("op_dew", "op_eve", "op_ann")) & (F.col("st.qty") >= 98), "left_anti")
    .filter(F.col("warehouse_id").isNotNull())
)

filtered = (
    without_matches.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") >= 150.7) & (~F.col("st.move_type").isin("ADJUST", "OUTBOUND")), "left_semi")
)

first_rows = filtered.filter(F.col("qty_rank") <= 5)

result = (
    first_rows
    .groupBy("qty_rank_quartile")
    .pivot("operator", ["op_eve", "op_chan", "op_bee"])
    .agg(F.avg("operator_or_unknown_distinct_operator_or_unknown"))
    .orderBy("qty_rank_quartile")
)
