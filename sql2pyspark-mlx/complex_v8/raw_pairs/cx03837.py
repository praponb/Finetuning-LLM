from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

combined_details_holder = (
    valid_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.joined_on").alias("holder_joined_on"))
)

with_details_holder = (
    combined_details_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
    )
)

without_matches = (
    with_details_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date").between(F.lit("2024-04-11").cast("date"), F.lit("2025-11-15").cast("date"))), "left_semi")
)

result = (
    without_matches
    .groupBy("end_date", "start_date")
    .pivot("holder_segment", ["SME", "RETAIL", "VIP"])
    .agg(F.sum("premium"))
)
