from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

with_activity = (
    valid
    .crossJoin(valid.agg(F.avg("unit_price").alias("overall_avg_unit_price")))
    .filter(F.col("unit_price") > F.col("overall_avg_unit_price"))
    .drop("overall_avg_unit_price")
)

enriched = (
    with_activity
    .filter(~F.col("operator").isin("op_bee", "op_dew"))
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "moved_at_year",
        "operator_len",
        "move_type_slug",
        F.concat_ws("|", F.col("move_type_slug"), F.col("operator")).alias("move_type_slug_operator_label"),
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
        F.round(F.col("unit_price") / F.nullif(F.col("operator_len"), F.lit(0)), 3).alias("unit_price_per_operator_len"),
    )
)

result = (
    enriched
    .groupBy("moved_at_year", "operator_len")
    .pivot("move_type", ["OUTBOUND", "ADJUST", "TRANSFER", "INBOUND"])
    .agg(F.max("unit_price"))
    .orderBy("moved_at_year", "operator_len")
)
