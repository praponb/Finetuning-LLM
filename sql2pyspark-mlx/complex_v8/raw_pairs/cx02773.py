from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.when(F.col("move_type").isin("ADJUST", "OUTBOUND"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
    )
)

enriched_sku = (
    scoped_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.weight_kg") >= 6.4), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.qty_per_unit_price",
        "s.move_type_group",
        "s.moved_at_day",
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

combined_details_sku = (
    enriched_sku.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.qty_per_unit_price",
        "s.move_type_group",
        "s.moved_at_day",
        "s.sku_code",
        "s.sku_unit_cost",
        F.col("sk.unit_cost").alias("sku_unit_cost_2"),
        F.col("sk.sku_code").alias("sku_sku_code"),
    )
)

result = (
    combined_details_sku
    .select(
        "id",
        "sku_id",
        "qty",
        "qty_per_unit_price",
        "move_type_group",
        "moved_at_day",
        "sku_code",
        "sku_unit_cost",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.when(F.col("move_type_group").isin("group_a", "group_b"), "group_a").when(F.col("move_type_group") == "other", "group_b").otherwise("other").alias("move_type_group_group"),
    )
    .orderBy(F.col("qty").desc_nulls_first())
)
