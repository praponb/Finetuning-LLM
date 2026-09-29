from pyspark.sql import functions as F

valid = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2025-09-27").cast("date")) & (F.col("product").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
        F.when(F.col("premium") >= 3719.7, "large").when(F.col("premium") >= 2013.8, "medium").otherwise("small").alias("premium_tier"),
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.premium_adj",
        "s.premium_tier",
        "s.product_clean",
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

product_stats = (
    with_details
    .groupBy("product")
    .agg(F.avg("premium_adj").alias("avg_premium_adj"))
)

compared = (
    with_details.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.premium_adj") > F.col("g.avg_premium_adj"))
    .select(
        "s.*",
        "g.avg_premium_adj",
        F.round(F.col("s.premium_adj") - F.col("g.avg_premium_adj"), 2).alias("premium_adj_minus_avg"),
    )
)

result = (
    compared
    .groupingSets(
        [["premium_tier", "status", "holder_id"], ["premium_tier"], []],
        "premium_tier", "status", "holder_id",
    )
    .agg(
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.min("holder_joined_on").alias("first_holder_joined_on"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("premium_tier").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
