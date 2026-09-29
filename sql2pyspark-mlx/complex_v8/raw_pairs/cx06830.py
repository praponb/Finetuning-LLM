from pyspark.sql import functions as F

valid = (
    df_policies
    .filter(F.col("end_date") < F.lit("2026-11-25").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status").isin("LAPSED", "ACTIVE"), "group_a").when(F.col("status") == "CANCELLED", "group_b").otherwise("other").alias("status_group"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

annotated = (
    valid
    .withColumn("product_or_unknown_ref", F.concat(F.col("product_or_unknown"), F.lit("#"), F.col("id").cast("string")))
)

with_children_claims = (
    annotated.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status_group",
        "s.product_or_unknown",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

qualified = (
    with_children_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.claim_adjuster_id")) & (F.hour(F.col("c.reported_at")).between(6, 12)), "left_anti")
)

line_level = (
    qualified.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status_group",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_claimed_amount",
        "s.claim_reported_at",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id_2"),
        "c.claim_date",
    )
)

joined_adjuster = (
    line_level.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id_2"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status_group",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_claimed_amount",
        "s.claim_reported_at",
        "s.claim_id_2",
        "s.claim_adjuster_id_2",
        "s.claim_date",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

exploded_detail = (
    joined_adjuster.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status_group",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_id_2",
        "s.claim_adjuster_id_2",
        "s.adjuster_name",
        "s.adjuster_level",
        F.col("c.id").alias("claim_id_3"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.reported_at").alias("claim_reported_at_2"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "start_date",
        "status_group",
        "claim_id",
        "claim_adjuster_id",
        "claim_id_2",
        "claim_adjuster_id_2",
        "adjuster_name",
        "adjuster_level",
        "claim_id_3",
        "claim_status",
        "claim_approved_amount",
        "claim_reported_at_2",
        F.trunc(F.col("end_date"), "MM").alias("end_date_month_start"),
        F.abs(F.col("adjuster_level") - 3).alias("adjuster_level_distance"),
    )
    .orderBy(F.col("start_date").desc())
)
