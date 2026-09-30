from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_len_2_by_qty = Window.partitionBy("move_type_len_2").orderBy(F.col("qty").desc(), "id")
w_move_type_len_2_by_qty_ties = Window.partitionBy("move_type_len_2").orderBy(F.col("qty").desc())
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")

cleaned = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
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
    )
)

labeled = (
    cleaned
    .filter(F.col("unit_price") > 160.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.abs(F.col("unit_price") - 164.1).alias("unit_price_distance"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len_2"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

with_stats = (
    labeled.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

windowed = (
    with_stats
    .select(
        "*",
        F.rank().over(w_move_type_len_2_by_qty).alias("qty_rank"),
        F.rank().over(w_move_type_len_2_by_qty_ties).alias("qty_rank_2"),
        F.max("unit_price_distance").over(w_warehouse_id_operator).alias("warehouse_id_operator_max_unit_price_distance"),
        F.lag("unit_price_distance", 1, 0).over(w_move_type_len_2_by_qty).alias("prev_unit_price_distance"),
        F.count(F.lit(1)).over(w_warehouse_id_operator).alias("rows_in_warehouse_id_operator"),
    )
)

top_rows = (
    windowed
    .filter((F.col("qty_rank") <= 3) & (F.col("move_type").isin("OUTBOUND", "ADJUST", "TRANSFER")))
)

result = (
    top_rows
    .filter(F.col("move_type").isin("TRANSFER", "OUTBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price_distance",
        "move_type_len_2",
        "move_count",
        "qty_rank_2",
        "warehouse_id_operator_max_unit_price_distance",
        "prev_unit_price_distance",
        "rows_in_warehouse_id_operator",
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
    .orderBy(F.col("operator").desc(), F.col("prev_unit_price_distance").desc(), "id")
    .limit(10)
)
