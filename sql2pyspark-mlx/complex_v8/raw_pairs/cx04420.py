from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_warehouse_id_moved_at_to_dat = Window.partitionBy("warehouse_id", F.to_date("moved_at")).orderBy(F.col("qty").desc())
w_operator_by_moved_at = Window.partitionBy("operator").orderBy("moved_at", "id")
w_warehouse_opened_on_month_op = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on"), "operator")

cleaned = (
    df_stock_moves
    .filter(F.col("qty") >= 102)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

with_counts = (
    cleaned.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

with_windows = (
    with_counts
    .select(
        "*",
        F.max("unit_price").over(w_move_type).alias("move_type_max_unit_price"),
        F.round(F.cume_dist().over(w_warehouse_id_moved_at_to_dat), 3).alias("qty_cume_dist"),
        F.round(F.percent_rank().over(w_warehouse_id_moved_at_to_dat), 3).alias("qty_percent_rank"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.row_number().over(w_operator_by_moved_at).alias("seq_no"),
    )
)

joined = (
    with_windows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.moved_at_label",
        "s.move_type_operator_label",
        "s.move_type_max_unit_price",
        "s.qty_percent_rank",
        "s.rows_in_move_type",
        "s.seq_no",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("unit_price").alias("max_move_unit_price"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

summarized_rows = (
    joined.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.max_move_unit_price", "st.first_moved_at")
)

move_stats_3 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count(F.when(F.col("move_type").isNotNull(), 1)).alias("move_move_type_count"),
    )
)

profiled = (
    summarized_rows.alias("s")
    .join(move_stats_3.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

metrics = (
    profiled
    .withColumn("qty_percent_rank_share_of_warehouse_opened_on_mo", F.round(F.col("qty_percent_rank") / F.nullif(F.sum("qty_percent_rank").over(w_warehouse_opened_on_month_op), F.lit(0)), 4))
    .withColumn("warehouse_opened_on_month_operator_avg_qty_perce", F.round(F.avg("qty_percent_rank").over(w_warehouse_opened_on_month_op), 2))
)

result = (
    metrics
    .select(
        "id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "move_type_operator_label",
        "qty_percent_rank",
        "rows_in_move_type",
        "seq_no",
        "warehouse_opened_on",
        "warehouse_city",
        "sku_code",
        "first_moved_at",
        "move_count",
        "qty_percent_rank_share_of_warehouse_opened_on_mo",
        "warehouse_opened_on_month_operator_avg_qty_perce",
        F.date_trunc("week", F.col("warehouse_opened_on")).alias("warehouse_opened_on_week"),
        F.abs(F.col("move_count") - 5).alias("move_count_distance"),
    )
    .orderBy("first_moved_at", F.col("move_count").desc_nulls_first())
)
