from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count_by_distinct_ids_ti = Window.partitionBy("row_count").orderBy(F.col("distinct_ids").desc())
w_move_type_group_move_type = Window.partitionBy("move_type_group", "move_type")
w_move_count_by_move_count_ful = Window.partitionBy("move_count").orderBy(F.col("move_count").desc(), "move_type", "warehouse_id", "sku_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_ids = Window.partitionBy("distinct_ids")

scoped_moves = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(10, 12))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
    )
)

totals = (
    scoped_moves
    .filter(F.col("move_type").isin("OUTBOUND", "ADJUST", "INBOUND"))
    .groupBy("move_type", "warehouse_id", "sku_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("qty") / F.nullif(F.sum("unit_price"), F.lit(0)), 3).alias("qty_to_unit_price_ratio"),
        F.sum(F.when(F.col("move_type") == "TRANSFER", F.col("qty")).otherwise(0)).alias("qty_move_type_transfer"),
        F.count(F.when(F.col("move_type").isin("TRANSFER", "ADJUST"), 1)).alias("n_move_type_in_transfer_adjust"),
        F.count("*").alias("row_count"),
    )
)

tagged = (
    totals
    .select(
        "move_type",
        "warehouse_id",
        "sku_id",
        "distinct_ids",
        "qty_to_unit_price_ratio",
        "qty_move_type_transfer",
        "row_count",
        F.round(F.col("qty_to_unit_price_ratio") * 0.9, 2).alias("qty_to_unit_price_ratio_adj"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.when(F.col("move_type").isin("ADJUST", "TRANSFER"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

windowed = (
    tagged
    .select(
        "move_type",
        "warehouse_id",
        "sku_id",
        "distinct_ids",
        "qty_to_unit_price_ratio",
        "qty_move_type_transfer",
        "qty_to_unit_price_ratio_adj",
        "move_type_len",
        F.round(F.percent_rank().over(w_row_count_by_distinct_ids_ti), 3).alias("distinct_ids_percent_rank"),
        F.max("qty_to_unit_price_ratio").over(w_move_type_group_move_type).alias("move_type_group_move_type_max_qty_to_unit_price_"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("move_count"),
    )
)

profiled = (
    windowed.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

metrics = (
    profiled
    .select(
        "*",
        F.nth_value("distinct_ids", 2).over(w_move_count_by_move_count_ful).alias("second_distinct_ids"),
        F.min("qty_move_type_transfer").over(w_distinct_ids).alias("distinct_ids_min_qty_move_type_transfer"),
        F.round(F.col("move_count") / F.nullif(F.sum("move_count").over(w_distinct_ids), F.lit(0)), 4).alias("move_count_share_of_distinct_ids"),
    )
)

with_children_moves = (
    metrics.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator") == "op_bee"), "inner")
    .select(
        "s.move_type",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty_to_unit_price_ratio",
        "s.qty_move_type_transfer",
        "s.last_moved_at",
        "s.move_count_share_of_distinct_ids",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    with_children_moves
    .rollup("move_operator", "warehouse_id")
    .agg(
        F.grouping("move_operator").alias("is_total_move_operator"),
        F.round(F.sum("move_count_share_of_distinct_ids"), 2).alias("total_move_count_share_of_distinct_ids"),
        F.max("qty_to_unit_price_ratio").alias("max_qty_to_unit_price_ratio"),
    )
    .orderBy(
        F.col("move_operator").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
