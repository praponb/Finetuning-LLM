from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((~F.col("product").isin("LIFE", "TRAVEL")) & (F.col("start_date") >= F.lit("2025-01-05").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

annotated = (
    prepared_policys
    .filter(F.col("product_prefix").isNotNull())
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.substring(F.trim(F.col("product_prefix")), 1, 4).alias("product_prefix_prefix"),
        F.round(F.col("premium") / F.nullif(F.col("premium_filled"), F.lit(0)), 3).alias("premium_per_premium_filled"),
    )
)

joined_holder = (
    annotated.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.product_prefix",
        "s.premium_filled",
        "s.product_prefix_prefix",
        "s.premium_per_premium_filled",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
        "p.holder_name",
    )
)

result = (
    joined_holder
    .rollup("holder_id", "holder_segment")
    .agg(F.array_sort(F.collect_set("holder_name")).alias("holder_name_list"))
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("holder_segment").asc_nulls_last(),
    )
)
