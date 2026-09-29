from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("sku_id") >= 13)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

per_group = (
    base
    .filter(F.month(F.col("moved_at")).isin(1, 2))
    .groupBy(
        F.date_format("moved_at", "yyyy-MM").alias("moved_at_ym"),
        "warehouse_id",
    )
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.round(100 * F.count(F.when(~F.col("operator").isin("op_dew", "op_chan"), 1)) / F.count("*"), 1).alias("pct_operator_not_in_op_dew_op_ch"),
        F.sum(F.when(F.col("move_type").isNotNull(), F.col("unit_price"))).alias("unit_price_move_type_is_not_null"),
    )
)

enriched = (
    per_group
    .filter(F.hour(F.col("last_moved_at")).between(8, 13))
    .select(
        "*",
        F.date_format(F.col("last_moved_at"), "MMM yyyy").alias("last_moved_at_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_moved_at")).alias("days_since_last_moved_at"),
        F.length(F.trim(F.col("moved_at_ym"))).alias("moved_at_ym_len"),
    )
)

result = (
    enriched
    .select(
        "moved_at_ym",
        "warehouse_id",
        "last_moved_at",
        "pct_operator_not_in_op_dew_op_ch",
        "unit_price_move_type_is_not_null",
        "last_moved_at_label",
        F.length(F.trim(F.col("last_moved_at_label"))).alias("last_moved_at_label_len"),
    )
    .orderBy(F.col("pct_operator_not_in_op_dew_op_ch").asc(), "moved_at_ym")
)
