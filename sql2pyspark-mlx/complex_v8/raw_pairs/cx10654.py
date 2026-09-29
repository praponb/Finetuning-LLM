from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("status") != "CANCELLED") & (F.col("product") != "HEALTH"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.last_day(F.col("start_date")).alias("start_date_month_end"),
        F.date_add(F.col("end_date"), 365).alias("end_date_plus_365d"),
    )
)

enriched_holder = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.start_date_month_end",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
    )
)

exploded_detail = (
    enriched_holder.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.holder_joined_on",
        "s.holder_region",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

qualified = (
    exploded_detail.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.claim_adjuster_id")) & (F.col("a.team") == "Motor") & (F.col("a.level") >= 1), "left_anti")
)

with_parent = (
    qualified.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.holder_joined_on",
        "s.holder_region",
        "s.claim_id",
        "s.claim_status",
        "s.claim_adjuster_id",
        "s.claim_approved_amount",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

filtered = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (F.col("c.claim_type") != "medical"), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "premium",
        "holder_joined_on",
        "holder_region",
        "claim_id",
        "claim_adjuster_id",
        "claim_approved_amount",
        "adjuster_name",
        "adjuster_team",
    )
    .orderBy(F.col("id").asc_nulls_last(), "adjuster_name")
)
