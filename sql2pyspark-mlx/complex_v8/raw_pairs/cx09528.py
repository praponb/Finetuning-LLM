from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")

prepared = (
    df_stock_moves
    .filter(F.col("sku_id") <= 7)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.size(F.collect_set("sku_id").over(w_warehouse_id_move_type)).alias("distinct_sku_id_in_warehouse_id_move_type"),
        F.round(F.col("operator_len") - F.avg("operator_len").over(w_warehouse_id_move_type), 2).alias("operator_len_vs_warehouse_id_move_type_avg"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("move_type") != "ADJUST", 1)).alias("move_move_type_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
        F.max("qty").alias("max_move_qty"),
    )
)

profiled = (
    ranked.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        "st.avg_move_unit_price",
        "st.max_move_qty",
    )
)

result = (
    profiled
    .cube("operator_len", "operator", "warehouse_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.countDistinct("move_type").alias("distinct_move_types"),
    )
)
