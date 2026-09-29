from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.month(F.col("moved_at")).isin(1, 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

joined_sku = (
    scoped_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.weight_kg") > 12.4)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

rollup_base = (
    joined_sku
    .groupBy("warehouse_id", "operator", "sku_id")
    .agg(
        F.round(100 * F.count(F.when(F.col("operator").isNotNull(), 1)) / F.count("*"), 1).alias("pct_operator_is_not_null"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.length(F.trim(F.col("sku_code"))) >= 7, F.col("sku_weight_kg"))).alias("sku_weight_kg_length_trim_sku_code_7"),
        F.count(F.when(F.upper(F.trim(F.col("sku_code"))).like("T%"), 1)).alias("n_upper_trim_sku_code_like_t"),
    )
)

result = (
    rollup_base
    .filter(F.col("sku_id") <= 14)
    .select(
        "warehouse_id",
        "operator",
        "pct_operator_is_not_null",
        "sku_weight_kg_length_trim_sku_code_7",
        "n_upper_trim_sku_code_like_t",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.coalesce(F.col("row_count"), F.lit(0)).alias("row_count_filled"),
    )
    .orderBy(
        F.col("operator_len").desc(),
        F.col("n_upper_trim_sku_code_like_t").desc_nulls_first(),
    )
)
