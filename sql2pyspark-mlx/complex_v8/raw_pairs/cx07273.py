from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_per_qty_pr = Window.orderBy(F.col("unit_price_per_qty").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_sku_id_operator_by_unit_pric = Window.partitionBy("sku_id", "operator").orderBy(F.col("unit_price_per_qty").desc_nulls_first(), "id").rowsBetween(-1, -1)

scoped = (
    df_stock_moves
    .filter((~F.col("move_type").isin("ADJUST", "INBOUND")) & (F.col("qty").between(99, 332)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.round(F.avg("unit_price_plus_qty").over(w_all_by_unit_price_per_qty_pr), 2).alias("unit_price_plus_qty_avg_3_prior"),
        F.round(F.avg("unit_price_plus_qty").over(w_sku_id_operator_by_unit_pric), 2).alias("unit_price_plus_qty_avg_1_prior"),
    )
)

move_type_stats = (
    sequenced
    .groupBy("move_type")
    .agg(F.avg("unit_price_plus_qty").alias("avg_unit_price_plus_qty"))
)

outliers = (
    sequenced.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price_plus_qty") > F.col("g.avg_unit_price_plus_qty"))
    .select(
        "s.*",
        "g.avg_unit_price_plus_qty",
        F.round(F.col("s.unit_price_plus_qty") - F.col("g.avg_unit_price_plus_qty"), 2).alias("unit_price_plus_qty_minus_avg"),
    )
)

with_activity = (
    outliers.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "OUTBOUND") & (F.col("st.sku_id").isNotNull()), "left_anti")
    .filter(F.col("unit_price_per_qty").between(3.0, 8.0))
)

result = (
    with_activity
    .select(
        "id",
        "move_type",
        "qty",
        "moved_at_plus_7d",
        "unit_price_plus_qty_avg_1_prior",
        "avg_unit_price_plus_qty",
        "unit_price_plus_qty_minus_avg",
        F.concat_ws("|", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.when(F.col("operator").isin("op_ann", "op_eve"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
    )
    .orderBy(F.col("avg_unit_price_plus_qty").desc())
)
