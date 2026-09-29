from pyspark.sql import functions as F

filtered_skus = (
    df_skus
    .filter((F.col("weight_kg") < 14.6) & (F.upper(F.trim(F.col("sku_code"))).like("E%")))
    .select(
        "id",
        "sku_code",
        "category",
        "labels",
        "is_active",
    )
)

per_group = (
    filtered_skus
    .groupBy("category")
    .agg(
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
        F.array_sort(F.collect_set("category")).alias("category_list"),
        F.countDistinct("category").alias("distinct_categorys"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

exploded = (
    per_group
    .select(
        "category",
        "category_list",
        "distinct_categorys",
        "n_rows",
        "distinct_ids",
        F.explode("sku_code_list").alias("sku_code_list_item"),
    )
)

result = (
    exploded
    .groupBy("distinct_categorys", "sku_code_list_item")
    .pivot("category", ["Electronics", "Beauty"])
    .agg(F.max("n_rows"))
)
