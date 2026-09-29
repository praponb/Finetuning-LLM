from pyspark.sql import functions as F

prepared_skus = (
    df_skus
    .filter((F.col("category").isin("Beauty", "Food")) | (F.col("unit_cost") > 38.3))
    .select(
        "id",
        "sku_code",
        "category",
        "labels",
        F.upper(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
    )
)

tagged = (
    prepared_skus
    .select(
        "*",
        F.concat(F.col("category"), F.lit("#"), F.col("id").cast("string")).alias("category_ref"),
        F.length(F.trim(F.col("category"))).alias("category_len"),
        F.length(F.trim(F.col("category"))).alias("category_len_2"),
    )
)

rollup_base = (
    tagged
    .groupBy("is_active_label", "category_len", "category")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("category_len_2") / F.nullif(F.sum("category_len"), F.lit(0)), 3).alias("category_len_2_to_category_len_ratio"),
    )
)

result = (
    rollup_base
    .groupBy("category_len")
    .pivot("category", ["Toys", "Apparel", "Beauty", "Food"])
    .agg(F.avg("cnt"))
    .orderBy("category_len")
)
