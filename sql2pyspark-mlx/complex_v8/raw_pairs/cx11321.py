from pyspark.sql import functions as F

matched = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.unit_price").alias("prior_unit_price_1d"),
    )
)

joined_sku = (
    matched.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.prior_rows_1d",
        "s.prior_unit_price_1d",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(~F.col("operator").isin("op_chan", "op_eve"), 1)).alias("move_operator_count"),
        F.count("*").alias("move_count"),
    )
)

with_stats = (
    joined_sku.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

sku_category_stats = (
    with_stats
    .groupBy("sku_category")
    .agg(F.avg("move_operator_count").alias("avg_move_operator_count"))
)

vs_group = (
    with_stats.alias("s")
    .join(sku_category_stats.alias("g"), F.col("g.sku_category") == F.col("s.sku_category"), "inner")
    .filter(F.col("s.move_operator_count") > F.col("g.avg_move_operator_count"))
    .select(
        "s.*",
        "g.avg_move_operator_count",
        F.round(F.col("s.move_operator_count") - F.col("g.avg_move_operator_count"), 2).alias("move_operator_count_minus_avg"),
    )
)

result = (
    vs_group
    .select("id", "move_count", "move_operator_count")
    .unpivot(
        ids=["id"],
        values=["move_count", "move_operator_count"],
        variableColumnName="field",
        valueColumnName="value",
    )
)
