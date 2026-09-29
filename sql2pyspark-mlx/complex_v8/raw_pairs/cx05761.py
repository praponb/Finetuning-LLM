from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-02-14").cast("date")) & (F.col("product") == "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.when(F.col("premium") >= 4417.0, "A").when(F.col("premium") >= 2013.8, "B").otherwise("C").alias("premium_tier"),
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
    )
)

annotated = (
    prepared
    .select(
        "id",
        "holder_id",
        "premium_tier",
        "start_date_quarter",
        (F.col("start_date_quarter") + F.expr("INTERVAL 7 DAYS")).alias("start_date_quarter_plus_7d"),
        F.coalesce(F.col("premium_tier"), F.lit("UNKNOWN")).alias("premium_tier_or_unknown"),
    )
)

enriched_holder = (
    annotated.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium_tier",
        "s.start_date_quarter",
        "s.premium_tier_or_unknown",
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    enriched_holder
    .select(
        "id",
        "premium_tier",
        "premium_tier_or_unknown",
        "holder_segment",
        F.when(F.col("premium_tier").isin("B", "A"), "group_a").when(F.col("premium_tier") == "C", "group_b").otherwise("other").alias("premium_tier_group"),
    )
    .orderBy(F.col("holder_segment").asc_nulls_last())
)
