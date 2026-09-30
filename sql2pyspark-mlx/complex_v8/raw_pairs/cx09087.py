from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_avg_move_unit_pric = Window.partitionBy("sku_id").orderBy("avg_move_unit_price", "id")
w_sku_id = Window.partitionBy("sku_id")
w_sku_id_by_avg_move_unit_pric_2 = Window.partitionBy("sku_id").orderBy("avg_move_unit_price", "id").rowsBetween(-3, Window.currentRow)

scoped = df_stock_moves.filter(F.col("warehouse_id").isNotNull())

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
        F.min("qty").alias("min_move_qty"),
    )
)

summarized_rows = (
    scoped.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        "st.avg_move_unit_price",
        "st.min_move_qty",
    )
)

with_trends = (
    summarized_rows
    .select(
        "*",
        F.ntile(10).over(w_sku_id_by_avg_move_unit_pric).alias("avg_move_unit_price_decile"),
        F.round(F.col("avg_move_unit_price") - F.avg("avg_move_unit_price").over(w_sku_id), 2).alias("avg_move_unit_price_vs_sku_id_avg"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.round(F.avg("qty").over(w_sku_id_by_avg_move_unit_pric_2), 2).alias("qty_avg_4"),
    )
)

per_group = (
    with_trends
    .groupBy("warehouse_id", "operator", "sku_id")
    .agg(
        F.sum(F.when(F.col("move_type").isin("TRANSFER", "OUTBOUND"), F.col("qty_avg_4"))).alias("qty_avg_4_move_type_in_transfer_outbou"),
        F.max("avg_move_unit_price").alias("max_avg_move_unit_price"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.count("*").alias("row_count"),
        F.round(F.avg("avg_move_unit_price"), 2).alias("avg_avg_move_unit_price"),
        F.avg("qty").alias("avg_qty_check"),
    )
    .filter(F.col("avg_qty_check") > 123)
    .drop("avg_qty_check")
)

without_matches = (
    per_group.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
)

result = (
    without_matches
    .select(
        "warehouse_id",
        "sku_id",
        "qty_avg_4_move_type_in_transfer_outbou",
        "max_avg_move_unit_price",
        "distinct_move_types",
        "move_type_list",
        "row_count",
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
    )
    .orderBy("distinct_move_types", "operator_prefix")
)
