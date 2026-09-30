from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2026-01-20").cast("date")) & (F.col("start_date").between(F.lit("2024-06-19").cast("date"), F.lit("2025-05-02").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
    )
)

joined_holder = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.start_date_label",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

combined_details_holder = (
    joined_holder.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.upper(F.trim(F.col("p.holder_name"))).like("D%")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.start_date_label",
        "s.holder_segment",
        "s.holder_joined_on",
        F.col("p.segment").alias("holder_segment_2"),
        F.col("p.joined_on").alias("holder_joined_on_2"),
    )
)

exploded_detail_claims = (
    combined_details_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") < 29359.4), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.holder_segment",
        "s.holder_joined_on",
        "s.holder_segment_2",
        "s.holder_joined_on_2",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    exploded_detail_claims
    .groupBy("claim_id")
    .pivot("status", ["EXPIRED", "CANCELLED", "LAPSED"])
    .agg(F.max("claim_approved_amount"))
    .orderBy("claim_id")
)
