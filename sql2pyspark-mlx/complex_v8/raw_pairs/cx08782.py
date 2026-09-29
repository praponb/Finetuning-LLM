from pyspark.sql import functions as F

filtered = (
    df_skus
    .filter(~F.col("is_active"))
    .select(
        "id",
        "category",
        "weight_kg",
        "labels",
        F.initcap(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
        (F.floor(F.col("unit_cost") / 50) * 50).alias("unit_cost_bucket"),
        F.length(F.trim(F.col("category"))).alias("category_len"),
    )
)

tagged = (
    filtered
    .filter(F.col("weight_kg").between(1.8, 15.2))
    .select(
        "id",
        "labels",
        "sku_code_clean",
        "unit_cost_bucket",
        "category_len",
        F.greatest(F.col("weight_kg"), F.col("unit_cost_bucket")).alias("greatest_weight_kg_unit_cost_bucket"),
        F.when(F.col("category").isin("Apparel", "Electronics"), "group_a").when(F.col("category") == "Beauty", "group_b").otherwise("other").alias("category_group"),
        F.upper(F.trim(F.col("category"))).alias("category_clean"),
    )
)

derived = (
    tagged
    .select(
        "id",
        "labels",
        "sku_code_clean",
        "unit_cost_bucket",
        "category_len",
        "category_clean",
        F.abs(F.col("category_len") - 6).alias("category_len_distance"),
        F.greatest(F.col("unit_cost_bucket"), F.col("category_len")).alias("greatest_unit_cost_bucket_category_len"),
    )
)

result = (
    derived
    .select("id", "greatest_unit_cost_bucket_category_len", "category_len")
    .unpivot(
        ids=["id"],
        values=["greatest_unit_cost_bucket_category_len", "category_len"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
