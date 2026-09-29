from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter((F.col("unit_price") <= 323.9) | (F.col("operator").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
    )
)

filtered = (
    prepared_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isin("op_eve", "op_bee", "op_ann")) & (F.month(F.col("st.moved_at")).isin(2, 3)), "left_anti")
)

joined = (
    filtered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        "s.qty_per_unit_price",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
)

with_stats = (
    joined.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.last_moved_at")
)

labeled = (
    with_stats
    .select(
        "*",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

derived = (
    labeled
    .select(
        "*",
        F.concat_ws(": ", F.col("move_type_or_unknown"), F.col("move_type")).alias("move_type_or_unknown_move_type_label"),
        (F.col("last_moved_at") + F.expr("INTERVAL 1 DAYS")).alias("last_moved_at_plus_1d"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("qty").alias("max_move_qty"),
        F.min("unit_price").alias("min_move_unit_price"),
        F.max("moved_at").alias("last_moved_at_2"),
    )
)

profiled = (
    derived.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.max_move_qty",
        "st.min_move_unit_price",
        "st.last_moved_at_2",
    )
)

result = (
    profiled
    .select("id", "max_move_qty", "qty")
    .unpivot(
        ids=["id"],
        values=["max_move_qty", "qty"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
