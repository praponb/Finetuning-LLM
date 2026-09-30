from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_operator_count_by_moved = Window.partitionBy("move_operator_count").orderBy(F.col("moved_at").desc())
w_warehouse_id = Window.partitionBy("warehouse_id")
w_move_operator_count_by_moved_2 = Window.partitionBy("move_operator_count").orderBy(F.col("moved_at").desc(), "id")

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.year(F.col("moved_at")).alias("moved_at_year"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count(F.when(F.col("operator").isin("op_chan", "op_ann", "op_dew"), 1)).alias("move_operator_count"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        "st.last_moved_at",
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.min("moved_at").alias("first_moved_at"))
)

with_stats_2 = (
    with_stats.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.first_moved_at")
)

with_details_warehouse = (
    with_stats_2.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

windowed = (
    with_details_warehouse
    .select(
        "*",
        F.round(F.percent_rank().over(w_move_operator_count_by_moved), 3).alias("moved_at_percent_rank"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
        F.row_number().over(w_move_operator_count_by_moved_2).alias("rn"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id), 2).alias("unit_price_vs_warehouse_id_avg"),
        F.max("moved_at_year").over(w_warehouse_id).alias("warehouse_id_max_moved_at_year"),
    )
)

result = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "moved_at_year",
        "move_count",
        "move_operator_count",
        "last_moved_at",
        "first_moved_at",
        "moved_at_percent_rank",
        "rn",
        "warehouse_id_max_moved_at_year",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_moved_at")).alias("days_since_last_moved_at"),
        F.add_months(F.col("warehouse_opened_on"), 6).alias("warehouse_opened_on_plus_6m"),
    )
    .orderBy(
        F.col("warehouse_id_max_moved_at_year").desc_nulls_first(),
        F.col("unit_price").asc(),
    )
)
