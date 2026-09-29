from pyspark.sql import functions as F

recent = df_policies.filter(F.col("premium") >= 2013.8)

annotated = (
    recent
    .filter(F.col("premium") >= 1051.9)
    .withColumn("status_ref", F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")))
)

with_details_holder = (
    annotated.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        F.col("p.segment").alias("holder_segment"),
    )
)

tagged = (
    with_details_holder
    .select(
        "*",
        F.coalesce(F.col("holder_segment"), F.lit("UNKNOWN")).alias("holder_segment_or_unknown"),
        F.when(F.col("product").isin("LIFE", "TRAVEL"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
    )
)

result = (
    tagged
    .rollup("product", "holder_segment")
    .agg(
        F.grouping_id().alias("gid"),
        F.max("premium").alias("max_premium"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("holder_segment").asc_nulls_last(),
    )
)
