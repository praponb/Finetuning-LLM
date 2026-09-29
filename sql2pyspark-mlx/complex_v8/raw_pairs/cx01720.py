from pyspark.sql import functions as F

prepared = (
    df_policyholders
    .filter((F.col("region") == "East") | (F.col("holder_name").isNotNull()))
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
        F.length(F.trim(F.col("segment"))).alias("segment_len"),
    )
)

qualified = (
    prepared.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.start_date") < F.lit("2025-03-06").cast("date")) & (F.col("p.premium") < 2945.9), "left_anti")
)

with_activity = (
    qualified.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.product") == "LIFE") & (F.col("p.status") == "LAPSED"), "left_semi")
    .filter(F.col("segment_len").between(3, 13))
)

tagged = (
    with_activity
    .select(
        "*",
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
    )
)

result = (
    tagged
    .cube("segment_len", "segment")
    .agg(F.count("*").alias("row_count"))
    .select(
        "segment_len",
        F.coalesce(F.col("segment"), F.lit("ALL")).alias("segment"),
        "row_count",
    )
    .orderBy(
        F.col("segment_len").asc_nulls_last(),
        F.col("segment").asc_nulls_last(),
    )
)
