from pyspark.sql import functions as F

prepared_skus = (
    df_skus
    .filter(F.col("weight_kg") > 3.7)
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

annotated = (
    prepared_skus
    .filter(F.col("weight_kg") < 14.6)
    .select(
        "*",
        F.when(F.col("category").isin("Toys", "Food"), "group_a").when(F.col("category") == "Apparel", "group_b").otherwise("other").alias("category_group"),
        F.when(F.col("category").isin("Electronics", "Beauty"), "group_a").when(F.col("category") == "Toys", "group_b").otherwise("other").alias("category_group_2"),
        F.lower(F.regexp_replace(F.trim(F.col("category")), " ", "_")).alias("category_slug"),
    )
)

per_label = (
    annotated
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        "category_group",
        "category_group_2",
        "category_slug",
        F.explode("labels").alias("label"),
    )
)

result = (
    per_label
    .cube("category_group_2", "category", "category_group")
    .agg(
        F.grouping("category_group_2").alias("is_total_category_group_2"),
        F.round(F.stddev("weight_kg"), 2).alias("stddev_weight_kg"),
        F.array_sort(F.collect_set("category")).alias("category_list"),
        F.round(F.avg("unit_cost"), 2).alias("avg_unit_cost"),
    )
    .select(
        F.coalesce(F.col("category_group_2"), F.lit("ALL")).alias("category_group_2"),
        F.coalesce(F.col("category"), F.lit("ALL")).alias("category"),
        F.coalesce(F.col("category_group"), F.lit("ALL")).alias("category_group"),
        "is_total_category_group_2",
        "stddev_weight_kg",
        "category_list",
        "avg_unit_cost",
    )
)
