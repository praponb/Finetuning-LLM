from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("qty") <= 300)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

enriched_sku = (
    scoped_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(~F.array_contains(F.col("sk.labels"), "fragile"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

with_activity = (
    enriched_sku.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id").isNotNull()), "left_semi")
)

result = (
    with_activity
    .groupingSets(
        [["operator", "sku_id"], ["operator"]],
        "operator", "sku_id",
    )
    .agg(
        F.sum(F.when(F.col("operator").isNotNull(), F.col("sku_weight_kg")).otherwise(0)).alias("sku_weight_kg_operator_is_not_null"),
        F.round(100 * F.count(F.when(F.col("operator") != "op_eve", 1)) / F.count("*"), 1).alias("pct_operator_op_eve"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .orderBy(F.col("operator").asc_nulls_last(), F.col("sku_id").asc_nulls_last())
)
