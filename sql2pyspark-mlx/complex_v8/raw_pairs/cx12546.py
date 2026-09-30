from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("unit_price") > 150.7) | (F.col("qty") > 90))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
    )
)

move_type_stats = (
    valid_moves
    .groupBy("move_type")
    .agg(F.percentile_approx("unit_price", 0.5).alias("median_unit_price"))
)

compared = (
    valid_moves.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price") >= F.col("g.median_unit_price"))
    .select(
        "s.*",
        "g.median_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.median_unit_price"), 2).alias("unit_price_minus_median"),
    )
)

with_parent_sku = (
    compared.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator_prefix",
        "s.unit_price_minus_median",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

screened = (
    with_parent_sku.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") < 384), "left_semi")
    .filter(F.col("unit_price_minus_median") > -150.0)
)

result = (
    screened
    .groupingSets(
        [["move_type", "warehouse_id"], ["move_type"], ["warehouse_id"], []],
        "move_type", "warehouse_id",
    )
    .agg(
        F.max("unit_price_minus_median").alias("max_unit_price_minus_median"),
        F.round(F.sum("unit_price_minus_median") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("unit_price_minus_median_to_qty_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
