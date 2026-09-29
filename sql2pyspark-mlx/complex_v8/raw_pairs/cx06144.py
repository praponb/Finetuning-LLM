from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2024-11-08").cast("date"), F.lit("2026-10-23").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.days_since_end_date",
        "s.product_prefix",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
    )
)

with_parent_holder = (
    combined_details.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.days_since_end_date",
        "s.product_prefix",
        "s.holder_risk_score",
        "s.holder_region",
        "s.holder_segment",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    with_parent_holder
    .filter(F.col("holder_region").isin("Central", "North"))
    .select(
        "id",
        "holder_id",
        "end_date",
        "days_since_end_date",
        "product_prefix",
        "holder_segment",
        "holder_joined_on",
        F.year(F.col("holder_joined_on")).alias("holder_joined_on_year"),
    )
    .orderBy(F.col("days_since_end_date").asc(), F.col("end_date").desc())
)
