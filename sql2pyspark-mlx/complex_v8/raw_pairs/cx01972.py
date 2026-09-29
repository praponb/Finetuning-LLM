from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter(F.col("premium") >= 2111.3)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
        F.when(F.col("premium") >= 4417.0, "HIGH").when(F.col("premium") >= 2013.8, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

enriched = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.premium_tier",
        "p.holder_name",
    )
)

enriched_2 = (
    enriched
    .select(
        "*",
        F.when(F.col("premium_tier").isin("MEDIUM", "LOW"), "group_a").when(F.col("premium_tier") == "HIGH", "group_b").otherwise("other").alias("premium_tier_group"),
        F.initcap(F.trim(F.col("premium_tier"))).alias("premium_tier_clean"),
    )
)

tagged = (
    enriched_2
    .select(
        "*",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.length(F.trim(F.col("holder_name"))).alias("holder_name_len"),
        F.when(F.col("product").isin("HEALTH", "AUTO"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
    )
)

result = (
    tagged
    .groupBy("product_group", "product")
    .pivot("premium_tier_group", ["other", "group_b"])
    .agg(F.max("holder_name_len"))
)
