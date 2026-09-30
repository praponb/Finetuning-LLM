from pyspark.sql import functions as F

scoped = (
    df_skus
    .filter(F.upper(F.trim(F.col("sku_code"))).like("T%"))
    .select(
        "id",
        "category",
        "weight_kg",
        "labels",
        "is_active",
        F.concat(F.col("category"), F.lit("#"), F.col("id").cast("string")).alias("category_ref"),
        F.upper(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
    )
)

screened = (
    scoped
    .crossJoin(scoped.agg(F.min("weight_kg").alias("overall_min_weight_kg")))
    .filter(F.col("weight_kg") > F.col("overall_min_weight_kg"))
    .drop("overall_min_weight_kg")
)

flattened = (
    screened
    .select(
        "id",
        "category",
        "weight_kg",
        "is_active",
        "category_ref",
        "sku_code_clean",
        F.posexplode("labels").alias("label_pos", "label"),
    )
)

result = (
    flattened
    .select(
        "id",
        "weight_kg",
        "label",
        F.round(F.col("label_pos") / F.nullif(F.col("weight_kg"), F.lit(0)), 3).alias("label_pos_per_weight_kg"),
        F.upper(F.trim(F.col("category"))).alias("category_clean"),
    )
    .orderBy(F.col("category_clean").desc_nulls_first(), F.col("id").desc())
)
