from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter((F.col("qty") >= 169) & (F.col("sku_id") <= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator") == "op_ann", 1)).alias("move_operator_count"),
    )
)

profiled = (
    base_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

sku_id_stats = (
    profiled
    .groupBy("sku_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

outliers = (
    profiled.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.unit_price") > F.col("g.avg_unit_price"))
    .select(
        "s.*",
        "g.avg_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.avg_unit_price"), 2).alias("unit_price_minus_avg"),
    )
)

result = (
    outliers
    .select("id", "unit_price_minus_avg", "avg_unit_price")
    .unpivot(
        ids=["id"],
        values=["unit_price_minus_avg", "avg_unit_price"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
