from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_qty_tier_operator_by_move_ty = Window.partitionBy("qty_tier", "operator").orderBy("move_type_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_qty_tier_operator_by_move_ty_2 = Window.partitionBy("qty_tier", "operator").orderBy("move_type_len", "id").rowsBetween(-6, -1)
w_move_type_len = Window.partitionBy("move_type_len")
w_all_by_qty = Window.orderBy("qty", "id")
w_operator = Window.partitionBy("operator")
w_all_by_move_count_2_prior_1 = Window.orderBy("move_count_2", "id").rowsBetween(-1, -1)

prepared_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("unit_price") >= 136.0))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.when(F.col("qty") >= 372, "large").when(F.col("qty") >= 116, "medium").otherwise("small").alias("qty_tier"),
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
    )
)

without_matches = (
    prepared_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator") != "op_bee") & (F.col("st.qty").between(145, 271)), "left_semi")
    .filter(F.col("move_type_prefix").isNotNull())
)

metrics = (
    without_matches
    .select(
        "*",
        F.sum("unit_price").over(w_qty_tier_operator_by_move_ty).alias("running_unit_price"),
        F.round(F.avg("move_type_len").over(w_qty_tier_operator_by_move_ty_2), 2).alias("move_type_len_avg_6_prior"),
        F.sum("move_type_len").over(w_move_type_len).alias("move_type_len_sum_move_type_len"),
        F.count(F.lit(1)).over(w_move_type_len).alias("rows_in_move_type_len"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"), F.count("*").alias("move_count_2"))
)

summarized_rows = (
    metrics.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

qualified = (
    summarized_rows.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
    .filter(F.col("move_count_2") <= 7)
)

with_trends = (
    qualified
    .select(
        "id",
        "move_type",
        "qty",
        "moved_at",
        "move_type_len",
        "move_type_prefix",
        "move_type_len_avg_6_prior",
        "move_type_len_sum_move_type_len",
        "move_count",
        "move_count_2",
        F.round(F.cume_dist().over(w_all_by_qty), 3).alias("qty_cume_dist"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
        F.round(F.avg("rows_in_move_type_len").over(w_all_by_move_count_2_prior_1), 2).alias("rows_in_move_type_len_avg_1_prior"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "move_count",
        "move_count_2",
        "move_type_len",
    )
    .unpivot(
        ids=["id"],
        values=["move_count", "move_count_2", "move_type_len"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
