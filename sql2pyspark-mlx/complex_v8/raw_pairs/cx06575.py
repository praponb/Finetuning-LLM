from pyspark.sql import functions as F

prepared_holders = (
    df_policyholders
    .filter((F.upper(F.trim(F.col("holder_name"))).like("B%")) | (F.col("joined_on").between(F.lit("2024-06-29").cast("date"), F.lit("2025-10-17").cast("date"))))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
        F.concat(F.col("segment"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref"),
    )
)

without_matches = (
    prepared_holders.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.end_date") < F.lit("2026-09-23").cast("date")) & (F.col("p.start_date") >= F.lit("2024-07-19").cast("date")), "left_semi")
    .filter(F.col("region") != "Central")
)

tagged = (
    without_matches
    .select(
        "*",
        F.round(F.col("days_since_joined_on") / F.nullif(F.col("risk_score"), F.lit(0)), 3).alias("days_since_joined_on_per_risk_score"),
        F.dayofweek(F.col("joined_on")).alias("joined_on_dayofweek"),
    )
)

result = (
    tagged
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "days_since_joined_on",
        "segment_ref",
        "days_since_joined_on_per_risk_score",
        "joined_on_dayofweek",
    )
    .orderBy(F.col("joined_on").desc(), F.col("risk_score").desc_nulls_first())
)
