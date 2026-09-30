from pyspark.sql import functions as F

base = (
    df_policyholders
    .filter(F.col("segment") == "RETAIL")
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
    )
)

screened = (
    base.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.premium") > 1541.8) & (F.col("p.start_date").between(F.lit("2024-05-07").cast("date"), F.lit("2025-05-28").cast("date"))), "left_semi")
    .filter(F.col("joined_on").isNotNull())
)

annotated = (
    screened
    .filter(F.length(F.trim(F.col("holder_name"))) >= 6)
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "risk_score_filled",
        F.concat(F.col("holder_name"), F.lit("#"), F.col("id").cast("string")).alias("holder_name_ref"),
        F.when(F.col("region").isin("South", "North"), "group_a").when(F.col("region") == "West", "group_b").otherwise("other").alias("region_group"),
        F.length(F.trim(F.col("holder_name"))).alias("holder_name_len"),
    )
)

screened_2 = (
    annotated.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.premium") <= 3532.7) & (F.col("p.end_date") < F.lit("2026-09-23").cast("date")), "left_anti")
)

result = (
    screened_2
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "holder_name_ref",
        "region_group",
        "holder_name_len",
    )
    .orderBy(F.col("holder_name_len").desc(), F.col("holder_name").desc(), "id")
    .limit(10)
)
