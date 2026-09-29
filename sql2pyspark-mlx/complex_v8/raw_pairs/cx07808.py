from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_sku_id_by_qty_full = Window.partitionBy("sku_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_qty_ties = Window.partitionBy("sku_id").orderBy("qty")

prepared_moves = (
    df_stock_moves
    .filter((F.col("unit_price") >= 60.4) & (F.col("warehouse_id") >= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("TRANSFER", "ADJUST"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

with_trends = (
    prepared_moves
    .select(
        "*",
        F.round(F.avg("unit_price").over(w_warehouse_id_operator), 2).alias("warehouse_id_operator_avg_unit_price"),
        F.first("least_qty_unit_price").over(w_sku_id_by_qty_full).alias("first_least_qty_unit_price"),
        F.size(F.collect_set("move_type").over(w_warehouse_id_operator)).alias("distinct_move_type_in_warehouse_id_operator"),
        F.dense_rank().over(w_sku_id_by_qty_ties).alias("qty_dense_rank"),
    )
)

tagged = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "least_qty_unit_price",
        "warehouse_id_operator_avg_unit_price",
        "first_least_qty_unit_price",
        "distinct_move_type_in_warehouse_id_operator",
        (F.col("least_qty_unit_price") + F.col("qty_dense_rank")).alias("least_qty_unit_price_plus_qty_dense_rank"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
    )
)

per_group = (
    tagged
    .groupBy("warehouse_id", F.year("moved_at").alias("moved_at_year"), "sku_id")
    .agg(
        F.round(100 * F.count(F.when(F.col("move_type").isin("TRANSFER", "OUTBOUND", "INBOUND"), 1)) / F.count("*"), 1).alias("pct_move_type_in_transfer_outbou"),
        F.max("qty").alias("max_qty"),
        F.sum("least_qty_unit_price_plus_qty_dense_rank").alias("sum_least_qty_unit_price_plus_qty_dense_rank_che"),
    )
    .filter(F.col("sum_least_qty_unit_price_plus_qty_dense_rank_che") > 179.2)
    .drop("sum_least_qty_unit_price_plus_qty_dense_rank_che")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("move_count"),
    )
)

with_stats = (
    per_group.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

result = (
    with_stats
    .select(
        "warehouse_id",
        "pct_move_type_in_transfer_outbou",
        "max_qty",
        "move_count",
        F.to_date(F.col("first_moved_at")).alias("first_moved_at_date"),
        (F.col("first_moved_at") + F.expr("INTERVAL 2 HOURS")).alias("first_moved_at_plus_2h"),
    )
    .orderBy("first_moved_at_plus_2h", F.col("max_qty").desc())
)
