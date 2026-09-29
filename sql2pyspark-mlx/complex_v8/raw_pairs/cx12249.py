from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_adj = Window.orderBy(F.col("unit_price_adj").desc(), "id", "move_id")
w_operator_warehouse_id_by_mov = Window.partitionBy("operator", "warehouse_id").orderBy("move_unit_price", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_warehouse_id_by_mov_2 = Window.partitionBy("operator", "warehouse_id").orderBy("move_unit_price", "id", "move_id")
w_operator_warehouse_id_by_mov_3 = Window.partitionBy("operator", "warehouse_id").orderBy("move_unit_price", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_id_by_qty_ties = Window.partitionBy("move_id").orderBy(F.col("qty").desc())
w_move_operator = Window.partitionBy("move_operator")
w_move_id_by_qty_running = Window.partitionBy("move_id").orderBy(F.col("qty").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_warehouse_id_by_un = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("unit_price_adj").desc(), "id", "move_id").rowsBetween(-1, Window.currentRow)
w_all_by_moved_at_date_full = Window.orderBy("moved_at_date", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_id_move_type = Window.partitionBy("move_id", "move_type")
w_move_type_warehouse_id_by_un_2 = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("unit_price_adj").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_stock_moves
    .filter(F.col("qty") < 355)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
    )
)

line_level = (
    recent.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.moved_at_date",
        "s.unit_price_adj",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.operator").alias("move_operator"),
    )
)

ranked = (
    line_level
    .select(
        "*",
        F.lead("qty").over(w_all_by_unit_price_adj).alias("next_qty"),
        F.last("move_unit_price").over(w_operator_warehouse_id_by_mov).alias("last_move_unit_price"),
        F.lag("unit_price_adj").over(w_operator_warehouse_id_by_mov_2).alias("prev_unit_price_adj"),
        F.min("unit_price_adj").over(w_operator_warehouse_id_by_mov_3).alias("min_unit_price_adj_so_far"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.rank().over(w_move_id_by_qty_ties).alias("qty_rank"),
        F.round(F.col("move_unit_price") / F.nullif(F.sum("move_unit_price").over(w_move_operator), F.lit(0)), 4).alias("move_unit_price_share_of_move_operator"),
        F.size(F.collect_set("operator").over(w_move_operator)).alias("distinct_operator_in_move_operator"),
        F.sum("move_unit_price").over(w_move_id_by_qty_running).alias("running_move_unit_price"),
    )
)

without_matches = (
    with_windows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.hour(F.col("st.moved_at")).between(6, 12)) & (F.col("st.move_type") == "ADJUST"), "left_semi")
    .filter(F.col("last_move_unit_price") < 396.4)
)

ranked_2 = (
    without_matches
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at_date",
        "unit_price_adj",
        "move_id",
        "move_unit_price",
        "move_warehouse_id",
        "last_move_unit_price",
        "prev_unit_price_adj",
        "min_unit_price_adj_so_far",
        "distinct_operator_in_move_operator",
        "running_move_unit_price",
        F.sum("qty_rank").over(w_move_type_warehouse_id_by_un).alias("qty_rank_sum_2"),
        F.nth_value("running_move_unit_price", 2).over(w_all_by_moved_at_date_full).alias("second_running_move_unit_price"),
        F.size(F.collect_set("operator").over(w_move_id_move_type)).alias("distinct_operator_in_move_id_move_type"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_move_id_move_type), F.lit(0)), 4).alias("qty_share_of_move_id_move_type"),
        F.first("running_move_unit_price", ignorenulls=True).over(w_move_type_warehouse_id_by_un_2).alias("first_known_running_move_unit_price"),
        F.first("next_qty", ignorenulls=True).over(w_move_type_warehouse_id_by_un_2).alias("first_known_next_qty"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.count(F.when(F.col("move_type") != "INBOUND", 1)).alias("move_move_type_count"),
    )
)

with_stats = (
    ranked_2.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

result = (
    with_stats
    .groupBy("move_warehouse_id")
    .pivot("move_type", ["TRANSFER", "ADJUST"])
    .agg(F.avg("min_unit_price_adj_so_far").alias("avg_v"), F.max("min_unit_price_adj_so_far").alias("max_v"))
    .orderBy("move_warehouse_id")
)
