from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_weight_kg = Window.orderBy("weight_kg", "id")
w_category_group_len = Window.partitionBy("category_group_len")

filtered_skus = (
    df_skus
    .filter(F.col("weight_kg") > 6.4)
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "is_active",
        F.when(F.col("category").isin("Beauty", "Food"), "group_a").when(F.col("category") == "Toys", "group_b").otherwise("other").alias("category_group"),
        F.concat_ws(" / ", F.col("category"), F.col("sku_code")).alias("category_sku_code_label"),
    )
)

enriched = (
    filtered_skus
    .filter(F.col("is_active"))
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "category_group",
        "category_sku_code_label",
        F.length(F.trim(F.col("category_group"))).alias("category_group_len"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "category_group",
        "category_group_len",
        F.row_number().over(w_all_by_weight_kg).alias("weight_kg_seq"),
        F.sum("category_group_len").over(w_category_group_len).alias("category_group_len_sum_category_group_len"),
    )
)

scored = (
    sequenced
    .filter(F.col("weight_kg") < 18.5)
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "category_group_len",
        "weight_kg_seq",
        "category_group_len_sum_category_group_len",
        (F.col("weight_kg") - F.col("category_group_len")).alias("weight_kg_minus_category_group_len"),
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
    )
)

totals = (
    scored
    .groupBy("category_or_unknown", "category", "category_group_len")
    .agg(
        F.count(F.when(F.upper(F.trim(F.col("sku_code"))).like("A%"), 1)).alias("n_upper_trim_sku_code_like_a"),
        F.sum(F.when(F.upper(F.trim(F.col("sku_code"))).like("T%"), 1).otherwise(0)).alias("n_upper_trim_sku_code_like_t"),
        F.percentile_approx("weight_kg_minus_category_group_len", 0.5).alias("median_weight_kg_minus_category_group_len"),
        F.sum("category_group_len_sum_category_group_len").alias("sum_category_group_len_sum_category_group_len_ch"),
    )
    .filter(F.col("sum_category_group_len_sum_category_group_len_ch") > 16)
    .drop("sum_category_group_len_sum_category_group_len_ch")
)

enriched_2 = (
    totals
    .filter(F.col("n_upper_trim_sku_code_like_a").between(2, 8))
    .select(
        "category_or_unknown",
        "category",
        "category_group_len",
        "n_upper_trim_sku_code_like_t",
        "median_weight_kg_minus_category_group_len",
        F.length(F.trim(F.col("category"))).alias("category_len"),
    )
)

labeled = (
    enriched_2
    .select(
        "*",
        F.greatest(F.col("category_len"), F.col("category_group_len")).alias("greatest_category_len_category_group_len"),
        F.lower(F.regexp_replace(F.trim(F.col("category")), " ", "_")).alias("category_slug"),
        F.round(F.col("category_group_len") / F.nullif(F.col("category_len"), F.lit(0)), 3).alias("category_group_len_per_category_len"),
    )
)

result = (
    labeled
    .filter(F.col("category").isin("Food", "Beauty", "Electronics"))
    .select(
        "category_or_unknown",
        "category",
        "category_group_len",
        "median_weight_kg_minus_category_group_len",
        "category_len",
        "greatest_category_len_category_group_len",
        "category_slug",
        "category_group_len_per_category_len",
    )
    .orderBy(
        F.col("category_group_len_per_category_len").desc_nulls_first(),
        F.col("greatest_category_len_category_group_len").desc(),
    )
)
