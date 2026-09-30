from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter((F.col("operator") == "op_bee") & (F.col("sku_id") >= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator").isin("op_eve", "op_chan", "op_ann"), 1)).alias("move_operator_count"),
        F.min("moved_at").alias("first_moved_at"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

profiled = (
    filtered_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        "st.first_moved_at",
        "st.last_moved_at",
    )
)

totals = (
    profiled
    .groupBy(
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
        "move_operator_count",
        "operator",
    )
    .agg(
        F.round(F.stddev("move_operator_count"), 2).alias("stddev_move_operator_count"),
        F.min("unit_price").alias("min_unit_price"),
        F.round(100 * F.count(F.when(F.col("move_type") != "ADJUST", 1)) / F.count("*"), 1).alias("pct_move_type_adjust"),
    )
)

annotated = (
    totals
    .select(
        "*",
        F.round(F.col("pct_move_type_adjust") / F.nullif(F.col("min_unit_price"), F.lit(0)), 3).alias("pct_move_type_adjust_per_min_unit_price"),
        F.round(F.col("move_operator_count") / F.nullif(F.col("min_unit_price"), F.lit(0)), 3).alias("move_operator_count_per_min_unit_price"),
    )
)

result = (
    annotated
    .groupBy("move_operator_count", "moved_at_month")
    .pivot("operator", ["op_bee", "op_eve", "op_chan"])
    .agg(F.max("pct_move_type_adjust").alias("max_v"), F.sum("pct_move_type_adjust").alias("sum_v"))
    .orderBy("move_operator_count", "moved_at_month")
)
