from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter((F.col("sku_id") >= 17) | (F.col("qty") >= 123))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

scored = (
    scoped
    .select(
        "*",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.when(F.col("unit_price") >= 298.9, "HIGH").when(F.col("unit_price") >= 161.4, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

with_details_sku = (
    scored.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    with_details_sku
    .groupingSets(
        [["sku_category", "sku_id", "move_type"], ["sku_category"], ["sku_id"]],
        "sku_category", "sku_id", "move_type",
    )
    .agg(
        F.grouping("sku_category").alias("is_total_sku_category"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
    )
)
