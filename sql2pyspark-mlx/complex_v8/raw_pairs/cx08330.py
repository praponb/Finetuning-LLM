from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("warehouse_id") >= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

labeled = (
    scoped
    .select(
        "*",
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

labeled_2 = (
    labeled
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        F.when(F.col("qty") >= 245, "large").when(F.col("qty") >= 168, "medium").otherwise("small").alias("qty_tier"),
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

result = (
    labeled_2
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "qty_tier",
        "operator_prefix",
        "operator_clean",
        F.initcap(F.trim(F.col("operator_clean"))).alias("operator_clean_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("operator_prefix")), " ", "_")).alias("operator_prefix_slug"),
    )
    .orderBy(F.col("id").desc())
)
