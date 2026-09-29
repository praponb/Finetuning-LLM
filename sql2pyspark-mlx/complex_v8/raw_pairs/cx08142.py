from pyspark.sql import functions as F

recent = (
    df_policies
    .filter((F.col("holder_id") <= 18) & (F.col("premium") > 2013.8))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_add(F.col("end_date"), 7).alias("end_date_plus_7d"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") >= 0.2), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date_plus_7d",
        "s.product_slug",
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

joined = (
    combined_details.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.length(F.trim(F.col("p.holder_name"))) >= 4), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.product_slug",
        "s.holder_segment",
        "s.holder_name",
        "s.holder_joined_on",
        F.col("p.holder_name").alias("holder_holder_name"),
        F.col("p.joined_on").alias("holder_joined_on_2"),
    )
)

result = (
    joined
    .rollup("holder_segment", "product", "holder_id")
    .agg(
        F.countDistinct("product").alias("distinct_products"),
        F.round(100 * F.count(F.when(F.col("product") != "HEALTH", 1)) / F.count("*"), 1).alias("pct_product_health"),
    )
    .orderBy(
        F.col("holder_segment").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
