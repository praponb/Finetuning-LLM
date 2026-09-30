from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.col("operator").isNotNull()) & (F.col("moved_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)

labeled = (
    cleaned_moves
    .withColumn("unit_price_filled_plus_qty_minus_unit_price", F.col("unit_price_filled") + F.col("qty_minus_unit_price"))
)

joined = (
    labeled.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.qty_minus_unit_price",
        "s.moved_at_label",
        "s.unit_price_filled_plus_qty_minus_unit_price",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

labeled_2 = (
    joined
    .filter(F.col("moved_at_label").isNotNull())
    .select(
        "*",
        F.round(F.col("sku_weight_kg") / F.nullif(F.col("qty_minus_unit_price"), F.lit(0)), 3).alias("sku_weight_kg_per_qty_minus_unit_price"),
        F.lower(F.trim(F.col("moved_at_label"))).alias("moved_at_label_clean"),
    )
)

result = (
    labeled_2
    .rollup("sku_id", "warehouse_id")
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.round(F.sum("unit_price"), 2).alias("total_unit_price"),
    )
)
