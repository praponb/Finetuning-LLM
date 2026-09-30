from pyspark.sql import functions as F

base = (
    df_policies
    .filter(~F.col("status").isin("CANCELLED", "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
    )
)

filtered = (
    base.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product") == "TRAVEL") & (F.col("p.end_date").between(F.lit("2025-10-12").cast("date"), F.lit("2027-01-03").cast("date"))), "left_semi")
)

qualified = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id") >= 3), "left_anti")
)

annotated = (
    qualified
    .select(
        "*",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.concat_ws("-", F.col("product"), F.col("status")).alias("product_status_label"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

qualified_2 = (
    annotated.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left_semi")
    .filter(F.col("holder_id") <= 7)
)

enriched_holder = (
    qualified_2.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.upper(F.trim(F.col("p.holder_name"))).like("G%")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        "s.product_len",
        "s.product_status_label",
        "s.status_or_unknown",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    enriched_holder
    .filter(F.col("end_date").between(F.lit("2025-10-12").cast("date"), F.lit("2026-12-05").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        "product_len",
        "product_status_label",
        "status_or_unknown",
        "holder_segment",
        "holder_joined_on",
        "holder_risk_score",
    )
    .orderBy("holder_segment")
)
