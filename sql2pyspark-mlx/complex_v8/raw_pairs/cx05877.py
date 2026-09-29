from pyspark.sql import functions as F

recent = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-08-27").cast("date"), F.lit("2025-09-24").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
    )
)

enriched = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

enriched_holder = (
    enriched.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.start_date_year",
        "s.holder_risk_score",
        "s.holder_joined_on",
        "p.holder_name",
    )
)

result = (
    enriched_holder
    .select(
        "id",
        "start_date",
        "start_date_year",
        "holder_risk_score",
        "holder_joined_on",
        "holder_name",
        F.initcap(F.trim(F.col("holder_name"))).alias("holder_name_clean"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("start_date_year")).alias("days_since_start_date_year"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
