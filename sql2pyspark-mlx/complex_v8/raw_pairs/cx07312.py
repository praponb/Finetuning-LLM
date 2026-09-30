from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_moved_at_month_move_typ = Window.partitionBy(F.month("move_moved_at"), "move_type_group")
w_warehouse_id_by_qty_running = Window.partitionBy("warehouse_id").orderBy("qty", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_qty_ties = Window.partitionBy("warehouse_id").orderBy("qty")
w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())
w_move_operator_move_sku_id_by = Window.partitionBy("move_operator", "move_sku_id").orderBy(F.col("move_unit_price_vs_move_moved_at_month_move_type").desc(), "id", "move_id")
w_qty_rank_move_moved_at_year = Window.partitionBy("qty_rank", F.year("move_moved_at"))
w_move_operator_move_sku_id_by_2 = Window.partitionBy("move_operator", "move_sku_id").orderBy(F.col("move_unit_price_vs_move_moved_at_month_move_type").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_move_unit_price_vs_move_move = Window.partitionBy("move_unit_price_vs_move_moved_at_month_move_type_2", "move_sku_id").orderBy("max_min_move_count_2_so_far_so_far")
w_move_unit_price_vs_move_move_2 = Window.partitionBy("move_unit_price_vs_move_moved_at_month_move_type_2", "move_sku_id").orderBy("max_min_move_count_2_so_far_so_far", "id", "move_id").rowsBetween(-2, -1)

valid_moves = (
    df_stock_moves
    .filter((F.col("qty").between(111, 469)) & (F.col("unit_price") > 61.2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.when(F.col("move_type").isin("TRANSFER", "OUTBOUND"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
        F.count(F.when(~F.col("operator").isin("op_eve", "op_chan"), 1)).alias("move_operator_count"),
    )
)

with_counts = (
    valid_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

detail_rows_moves = (
    with_counts.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.move_type_group",
        "s.move_count",
        "s.move_count_2",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

ranked = (
    detail_rows_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "move_count_2",
        "move_id",
        "move_operator",
        "move_sku_id",
        "move_unit_price",
        "move_moved_at",
        F.count(F.lit(1)).over(w_move_moved_at_month_move_typ).alias("rows_in_move_moved_at_month_move_type_group"),
        F.min("move_count_2").over(w_warehouse_id_by_qty_running).alias("min_move_count_2_so_far"),
        F.rank().over(w_warehouse_id_by_qty_ties).alias("qty_rank"),
        F.rank().over(w_all_by_qty_ties).alias("qty_rank_2"),
        F.max("move_count_2").over(w_move_moved_at_month_move_typ).alias("move_moved_at_month_move_type_group_max_move_cou"),
        F.round(F.col("move_unit_price") - F.avg("move_unit_price").over(w_move_moved_at_month_move_typ), 2).alias("move_unit_price_vs_move_moved_at_month_move_type"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "move_id",
        "move_operator",
        "move_sku_id",
        "move_moved_at",
        "min_move_count_2_so_far",
        "qty_rank_2",
        "move_moved_at_month_move_type_group_max_move_cou",
        "move_unit_price_vs_move_moved_at_month_move_type",
        (F.col("qty") - F.lag("qty").over(w_move_operator_move_sku_id_by)).alias("qty_change"),
        F.round(F.col("min_move_count_2_so_far") / F.nullif(F.sum("min_move_count_2_so_far").over(w_qty_rank_move_moved_at_year), F.lit(0)), 4).alias("min_move_count_2_so_far_share_of_qty_rank_move_m"),
        F.max("min_move_count_2_so_far").over(w_move_operator_move_sku_id_by_2).alias("max_min_move_count_2_so_far_so_far"),
        F.count(F.lit(1)).over(w_qty_rank_move_moved_at_year).alias("rows_in_qty_rank_move_moved_at_year"),
        F.dense_rank().over(w_move_operator_move_sku_id_by).alias("move_unit_price_vs_move_moved_at_month_move_type_2"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.round(F.avg("qty_rank_2").over(w_sku_id_warehouse_id), 2).alias("sku_id_warehouse_id_avg_qty_rank_2"),
        F.count(F.lit(1)).over(w_sku_id_warehouse_id).alias("rows_in_sku_id_warehouse_id"),
        F.round(F.cume_dist().over(w_move_unit_price_vs_move_move), 3).alias("max_min_move_count_2_so_far_so_far_cume_dist"),
        F.min("min_move_count_2_so_far").over(w_sku_id_warehouse_id).alias("sku_id_warehouse_id_min_min_move_count_2_so_far"),
        F.round(F.avg("qty_rank_2").over(w_move_unit_price_vs_move_move_2), 2).alias("qty_rank_2_avg_2_prior"),
    )
)

annotated = (
    sequenced
    .filter(F.col("max_min_move_count_2_so_far_so_far_cume_dist") >= 0.5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "move_id",
        "move_operator",
        "move_sku_id",
        "move_moved_at",
        "min_move_count_2_so_far",
        "min_move_count_2_so_far_share_of_qty_rank_move_m",
        "rows_in_qty_rank_move_moved_at_year",
        "sku_id_warehouse_id_avg_qty_rank_2",
        "rows_in_sku_id_warehouse_id",
        "max_min_move_count_2_so_far_so_far_cume_dist",
        "sku_id_warehouse_id_min_min_move_count_2_so_far",
        "qty_rank_2_avg_2_prior",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        (F.col("min_move_count_2_so_far") - F.col("qty_rank_2_avg_2_prior")).alias("min_move_count_2_so_far_minus_qty_rank_2_avg_2_p"),
    )
)

result = (
    annotated
    .filter(~F.col("operator").isin("op_bee", "op_ann"))
    .select(
        "id",
        "move_type",
        "operator",
        "move_id",
        "move_sku_id",
        "move_moved_at",
        "min_move_count_2_so_far",
        "rows_in_qty_rank_move_moved_at_year",
        "rows_in_sku_id_warehouse_id",
        "max_min_move_count_2_so_far_so_far_cume_dist",
        "sku_id_warehouse_id_min_min_move_count_2_so_far",
        "qty_rank_2_avg_2_prior",
        "move_type_len",
        "min_move_count_2_so_far_minus_qty_rank_2_avg_2_p",
    )
    .orderBy(
        F.col("rows_in_sku_id_warehouse_id").desc(),
        F.col("max_min_move_count_2_so_far_so_far_cume_dist").desc(),
    )
)
