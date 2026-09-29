from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("qty").between(137, 271)) & (F.col("warehouse_id") <= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
)

derived = (
    cleaned
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "moved_at_hour",
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
    )
)

with_details = (
    derived.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.moved_at_hour",
        "s.operator_prefix",
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

sku_id_stats = (
    with_details
    .groupBy("sku_id")
    .agg(F.max("sku_unit_cost").alias("max_sku_unit_cost"))
)

vs_group = (
    with_details.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.sku_unit_cost") == F.col("g.max_sku_unit_cost"))
    .select(
        "s.*",
        "g.max_sku_unit_cost",
        F.round(F.col("s.sku_unit_cost") - F.col("g.max_sku_unit_cost"), 2).alias("sku_unit_cost_minus_max"),
    )
)

combined_details = (
    vs_group.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.unit_cost") < 239.3)
    .select(
        "s.id",
        "s.sku_id",
        "s.operator",
        "s.moved_at_hour",
        "s.sku_code",
        "s.sku_unit_cost",
        "s.max_sku_unit_cost",
        F.col("sk.unit_cost").alias("sku_unit_cost_2"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "sku_id",
        "operator",
        "moved_at_hour",
        "sku_code",
        "sku_unit_cost",
        "max_sku_unit_cost",
        F.quarter(F.col("moved_at_hour")).alias("moved_at_hour_quarter"),
        F.to_date(F.col("moved_at_hour")).alias("moved_at_hour_date"),
    )
)
