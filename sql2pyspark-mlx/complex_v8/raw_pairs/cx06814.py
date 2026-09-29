from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.col("product").isin("LIFE", "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.when(F.col("premium") >= 3465.4, "HIGH").when(F.col("premium") >= 2096.6, "MEDIUM").otherwise("LOW").alias("premium_tier"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

labeled = (
    scoped_policys
    .filter(F.col("status_or_unknown").isNotNull())
    .select(
        "*",
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
        F.date_trunc("week", F.col("start_date")).alias("start_date_week"),
        F.date_format(F.col("start_date"), "yyyy-MM-dd").alias("start_date_label"),
    )
)

joined_holder = (
    labeled.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    joined_holder
    .cube("start_date_quarter", "holder_id")
    .agg(
        F.round(F.sum("start_date_quarter") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("start_date_quarter_to_premium_ratio"),
        F.min("start_date_quarter").alias("min_start_date_quarter"),
        F.countDistinct("premium_tier").alias("distinct_premium_tiers"),
    )
    .orderBy(
        F.col("start_date_quarter").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
