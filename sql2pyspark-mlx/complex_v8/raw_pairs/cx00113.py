from pyspark.sql import functions as F

recent_skus = (
    df_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.round(F.col("weight_kg") * 1.5, 2).alias("weight_kg_adj"),
        F.concat_ws("|", F.col("sku_code"), F.col("category")).alias("sku_code_category_label"),
    )
)

qualified = (
    recent_skus
    .crossJoin(recent_skus.agg(F.avg("unit_cost").alias("overall_avg_unit_cost")))
    .filter(F.col("unit_cost") > F.col("overall_avg_unit_cost"))
    .drop("overall_avg_unit_cost")
)

result = (
    qualified
    .groupingSets(
        [["is_active", "category"], ["is_active"]],
        "is_active", "category",
    )
    .agg(F.grouping_id().alias("gid"), F.count("*").alias("n_rows"))
    .select(
        "is_active",
        F.coalesce(F.col("category"), F.lit("ALL")).alias("category"),
        "gid",
        "n_rows",
    )
    .orderBy(F.col("is_active").asc_nulls_last(), F.col("category").asc_nulls_last())
)
