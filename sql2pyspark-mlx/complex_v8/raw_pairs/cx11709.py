from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count_move_sku_id = Window.partitionBy("row_count", "move_sku_id")
w_all_by_row_count_prior_1 = Window.orderBy(F.col("row_count").desc(), "move_sku_id", "move_count", "move_type").rowsBetween(-1, -1)
w_move_type_by_n_move_type_is = Window.partitionBy("move_type").orderBy("n_move_type_is_not_null", "move_sku_id", "move_count", "move_type")

recent_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("operator").isin("op_chan", "op_dew", "op_eve")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

exploded_detail_moves = (
    recent_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id") <= 15), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
    )
)

combined_details_move = (
    exploded_detail_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_id",
        "s.move_move_type",
        "s.move_sku_id",
        "s.move_operator",
        F.col("st.sku_id").alias("move_sku_id_2"),
    )
)

combined_details = (
    combined_details_move.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.move_sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        "s.move_id",
        "s.move_move_type",
        "s.move_sku_id",
        "s.move_sku_id_2",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("qty").alias("min_move_qty"),
        F.count("*").alias("move_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
    )
)

profiled = (
    combined_details.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.move_sku_id"), "left")
    .select(
        "s.*",
        "st.min_move_qty",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.avg_move_unit_price",
    )
)

rollup_base = (
    profiled
    .groupBy("move_sku_id", "move_count", "move_type")
    .agg(
        F.sum("min_move_qty").alias("total_min_move_qty"),
        F.sum(F.when(F.col("move_type") == "OUTBOUND", 1).otherwise(0)).alias("n_move_type_outbound"),
        F.sum(F.when(F.col("move_type").isNotNull(), 1).otherwise(0)).alias("n_move_type_is_not_null"),
        F.sum(F.when(F.col("sku_category") == "Electronics", 1).otherwise(0)).alias("n_sku_category_electronics"),
        F.round(F.sum("move_count") / F.nullif(F.sum("unit_price"), F.lit(0)), 3).alias("move_count_to_unit_price_ratio"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 2)
)

with_windows = (
    rollup_base
    .select(
        "move_sku_id",
        "move_count",
        "move_type",
        "n_move_type_outbound",
        "n_move_type_is_not_null",
        "n_sku_category_electronics",
        "row_count",
        F.min("n_move_type_is_not_null").over(w_row_count_move_sku_id).alias("row_count_move_sku_id_min_n_move_type_is_not_nul"),
        F.max("move_count").over(w_row_count_move_sku_id).alias("row_count_move_sku_id_max_move_count"),
        F.sum("n_sku_category_electronics").over(w_all_by_row_count_prior_1).alias("n_sku_category_electronics_sum_1_prior"),
        F.min("move_count").over(w_row_count_move_sku_id).alias("row_count_move_sku_id_min_move_count"),
        F.count(F.lit(1)).over(w_row_count_move_sku_id).alias("rows_in_row_count_move_sku_id"),
        F.lag("n_move_type_is_not_null").over(w_move_type_by_n_move_type_is).alias("prev_n_move_type_is_not_null"),
    )
)

result = (
    with_windows
    .groupBy("n_move_type_outbound")
    .pivot("move_type", ["INBOUND", "ADJUST"])
    .agg(F.sum("prev_n_move_type_is_not_null"))
)
