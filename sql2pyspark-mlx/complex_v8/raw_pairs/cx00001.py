from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.col("premium") >= 2096.6)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.abs(F.col("premium") - 3465.4).alias("premium_distance"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

combined_details = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.premium_distance",
        "s.status_or_unknown",
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
    )
)

with_details = (
    combined_details.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.holder_name").alias("holder_holder_name"),
        F.col("p.region").alias("holder_region"),
    )
)

screened = (
    with_details.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status").isin("PAID", "APPROVED")) & (F.col("c.claimed_amount") <= 31752.9), "left_semi")
)

result = (
    screened
    .filter(F.col("premium_distance").between(1500.0, 4250.0))
    .select(
        "id",
        "holder_id",
        "product",
        "premium_distance",
        "status_or_unknown",
        "holder_segment",
        "holder_name",
        "holder_holder_name",
        "holder_region",
        F.lower(F.trim(F.col("holder_segment"))).alias("holder_segment_clean"),
    )
    .orderBy(F.col("product").asc(), "id")
    .limit(5)
)
