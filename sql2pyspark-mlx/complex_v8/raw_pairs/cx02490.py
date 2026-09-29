from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-07-19").cast("date")) & (F.col("end_date").between(F.lit("2025-12-05").cast("date"), F.lit("2026-11-25").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

enriched_holder = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") >= 0.1)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.premium_filled",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

derived = (
    enriched_holder
    .select(
        "id",
        "product",
        "start_date",
        "end_date",
        "premium_filled",
        "holder_segment",
        "holder_risk_score",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
    )
)

annotated = (
    derived
    .select(
        "*",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.round(F.col("premium_filled") / F.nullif(F.col("holder_risk_score"), F.lit(0)), 3).alias("premium_filled_per_holder_risk_score"),
    )
)

result = (
    annotated
    .cube("product_or_unknown", "holder_segment")
    .agg(
        F.percentile_approx("holder_risk_score", 0.5).alias("median_holder_risk_score"),
    )
)
