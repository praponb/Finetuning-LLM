from pyspark.sql import functions as F

filtered_skus = (
    df_skus
    .filter(F.col("category") != "Beauty")
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

rollup_base = (
    filtered_skus
    .groupBy("category")
    .agg(
        F.array_sort(F.collect_set("category")).alias("category_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("unit_cost").alias("sum_unit_cost_check"),
    )
    .filter(F.col("sum_unit_cost_check") > 108.5)
    .drop("sum_unit_cost_check")
)

result = (
    rollup_base
    .filter(F.col("category") == "Food")
    .select(
        "category",
        "category_list",
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
        F.coalesce(F.col("distinct_ids"), F.lit(0)).alias("distinct_ids_filled"),
    )
    .orderBy(F.col("category_or_unknown").desc(), "category")
    .limit(5)
)
