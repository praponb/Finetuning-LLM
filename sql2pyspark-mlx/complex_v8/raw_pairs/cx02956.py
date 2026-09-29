from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_to_date_warehouse_i = Window.partitionBy(F.to_date("moved_at"), "warehouse_id").orderBy(F.col("moved_at").desc(), "id")
w_all_by_qty = Window.orderBy(F.col("qty").asc_nulls_last(), "id")

filtered_moves = (
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
        F.month(F.col("moved_at")).alias("moved_at_month"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator") != "op_chan", 1)).alias("move_operator_count"),
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
    )
)

with_counts = (
    filtered_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

windowed = (
    with_counts
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "moved_at_month",
        "move_operator_count",
        "move_count",
        "move_count_2",
        F.round(F.percent_rank().over(w_moved_at_to_date_warehouse_i), 3).alias("moved_at_percent_rank"),
        F.row_number().over(w_all_by_qty).alias("rn"),
    )
)

sku_id_stats = (
    windowed
    .groupBy("sku_id")
    .agg(F.max("move_operator_count").alias("max_move_operator_count"))
)

compared = (
    windowed.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.move_operator_count") == F.col("g.max_move_operator_count"))
    .select(
        "s.*",
        "g.max_move_operator_count",
        F.round(F.col("s.move_operator_count") - F.col("g.max_move_operator_count"), 2).alias("move_operator_count_minus_max"),
    )
)

result = (
    compared
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "moved_at_month",
        "move_count",
        "moved_at_percent_rank",
        "rn",
        "max_move_operator_count",
        "move_operator_count_minus_max",
        F.least(F.col("moved_at_month"), F.col("max_move_operator_count")).alias("least_moved_at_month_max_move_operator_count"),
    )
    .orderBy("moved_at_month", F.col("unit_price").asc_nulls_last())
)
