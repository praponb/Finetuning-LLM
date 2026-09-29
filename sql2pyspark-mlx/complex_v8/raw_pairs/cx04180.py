from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type = Window.partitionBy("move_type")
w_all_by_unit_price = Window.orderBy("unit_price", "id")

base_moves = (
    df_stock_moves
    .filter(F.col("unit_price").between(111.7, 341.5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("OUTBOUND", "INBOUND"), "group_a").when(F.col("move_type") == "TRANSFER", "group_b").otherwise("other").alias("move_type_group"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.sum("qty").alias("total_move_qty"),
        F.count("*").alias("move_count"),
    )
)

summarized_rows = (
    base_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        "st.total_move_qty",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

with_details_warehouse = (
    summarized_rows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.move_type_group",
        "s.unit_price_plus_qty",
        "s.unit_price_minus_qty",
        "s.first_moved_at",
        "s.total_move_qty",
        "s.move_count",
        F.col("w.city").alias("warehouse_city"),
    )
)

labeled = (
    with_details_warehouse
    .filter(F.col("warehouse_id") >= 5)
    .withColumn("move_count_distance", F.abs(F.col("move_count") - 5))
)

metrics = (
    labeled
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "unit_price_plus_qty",
        "unit_price_minus_qty",
        "first_moved_at",
        "total_move_qty",
        "warehouse_city",
        F.sum("move_count").over(w_all_by_unit_price_running).alias("running_move_count"),
        F.round(F.avg("unit_price").over(w_move_type), 2).alias("move_type_avg_unit_price"),
        F.round(F.cume_dist().over(w_all_by_unit_price), 3).alias("unit_price_cume_dist"),
        F.round(F.col("unit_price_minus_qty") - F.avg("unit_price_minus_qty").over(w_move_type), 2).alias("unit_price_minus_qty_vs_move_type_avg"),
        F.dense_rank().over(w_all_by_unit_price).alias("unit_price_dense_rank"),
    )
)

rollup_base = (
    metrics
    .groupBy(
        "warehouse_city",
        F.date_format("first_moved_at", "yyyy-MM").alias("first_moved_at_ym"),
    )
    .agg(
        F.round(F.stddev("unit_price_minus_qty_vs_move_type_avg"), 2).alias("stddev_unit_price_minus_qty_vs_move_type_avg"),
        F.array_sort(F.collect_set("warehouse_city")).alias("warehouse_city_list"),
        F.max("unit_price_cume_dist").alias("max_unit_price_cume_dist"),
    )
)

result = (
    rollup_base
    .select("warehouse_city", "max_unit_price_cume_dist")
    .orderBy(
        F.col("max_unit_price_cume_dist").asc(),
        F.col("warehouse_city").asc_nulls_last(),
    )
)
