from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter(F.col("sku_id") >= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.month(F.col("moved_at")).alias("moved_at_month"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.concat_ws(": ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

derived = (
    base_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "*",
        F.round(F.col("unit_price") / F.nullif(F.col("moved_at_month"), F.lit(0)), 3).alias("unit_price_per_moved_at_month"),
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label_2"),
        F.date_trunc("quarter", F.col("moved_at")).alias("moved_at_quarter"),
    )
)

result = (
    derived
    .groupingSets(
        [["move_type", "operator"], ["move_type"]],
        "move_type", "operator",
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.percentile_approx("unit_price_per_moved_at_month", 0.5).alias("median_unit_price_per_moved_at_month"),
        F.sum(F.when(~F.col("move_type").isin("TRANSFER", "OUTBOUND"), 1).otherwise(0)).alias("n_move_type_not_in_transfer_ou"),
    )
    .select(
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        "min_unit_price",
        "median_unit_price_per_moved_at_month",
        "n_move_type_not_in_transfer_ou",
    )
    .orderBy(F.col("move_type").asc_nulls_last(), F.col("operator").asc_nulls_last())
)
