from pyspark.sql import functions as F

base = (
    df_skus
    .filter((~F.col("is_active")) & (F.col("unit_cost") >= 60.9))
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

annotated = (
    base
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.round(F.col("unit_cost") / F.nullif(F.col("weight_kg"), F.lit(0)), 3).alias("unit_cost_per_weight_kg"),
        F.concat(F.col("sku_code"), F.lit("#"), F.col("id").cast("string")).alias("sku_code_ref"),
        F.array_join(F.array_sort(F.col("labels")), ",").alias("labels_text"),
    )
)

result = (
    annotated
    .filter(F.upper(F.trim(F.col("sku_code"))).like("E%"))
    .select(
        "id",
        "labels",
        "unit_cost_per_weight_kg",
        "sku_code_ref",
        "labels_text",
        F.lower(F.regexp_replace(F.trim(F.col("sku_code")), " ", "_")).alias("sku_code_slug"),
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
