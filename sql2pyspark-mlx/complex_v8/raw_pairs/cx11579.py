from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("unit_price") >= 200.7)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("operator_ref"),
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.round(F.avg("qty"), 2).alias("avg_move_qty"),
        F.max("unit_price").alias("max_move_unit_price"),
        F.count("*").alias("move_count"),
    )
)

profiled = (
    scoped_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.avg_move_qty",
        "st.max_move_unit_price",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

filtered = (
    profiled
    .crossJoin(profiled.agg(F.avg("avg_move_qty").alias("overall_avg_avg_move_qty")))
    .filter(F.col("avg_move_qty") > F.col("overall_avg_avg_move_qty"))
    .drop("overall_avg_avg_move_qty")
)

result = (
    filtered
    .select("id", "max_move_unit_price", "unit_price")
    .unpivot(
        ids=["id"],
        values=["max_move_unit_price", "unit_price"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
