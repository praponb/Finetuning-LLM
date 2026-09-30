from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_id_status_by_claim_app = Window.partitionBy("claim_id", "status").orderBy("claim_approved_amount", "id", "claim_id").rowsBetween(-2, -1)
w_claim_id_status_by_claim_app_2 = Window.partitionBy("claim_id", "status").orderBy("claim_approved_amount", "id", "claim_id")

prepared = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_children_claims = (
    prepared.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") == "IN_REVIEW"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.status",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

labeled = (
    with_children_claims
    .filter(F.col("premium") > 2013.8)
    .select(
        "id",
        "end_date",
        "status",
        "claim_id",
        "claim_approved_amount",
        F.substring(F.trim(F.col("claim_status")), 1, 2).alias("claim_status_prefix"),
        F.date_trunc("month", F.col("end_date")).alias("end_date_month"),
        (F.col("premium") + F.col("claim_approved_amount")).alias("premium_plus_claim_approved_amount"),
    )
)

sequenced = (
    labeled
    .select(
        "*",
        F.round(F.avg("claim_approved_amount").over(w_claim_id_status_by_claim_app), 2).alias("claim_approved_amount_avg_2_prior"),
        F.lead("end_date", 2).over(w_claim_id_status_by_claim_app_2).alias("next2_end_date"),
        F.ntile(5).over(w_claim_id_status_by_claim_app_2).alias("claim_approved_amount_quintile"),
        F.row_number().over(w_claim_id_status_by_claim_app_2).alias("claim_approved_amount_seq"),
    )
)

combined_details_claim = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .filter(F.col("c.adjuster_id") <= 4)
    .select("s.*", F.col("c.adjuster_id").alias("claim_adjuster_id"))
)

joined = (
    combined_details_claim.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id"), "inner")
    .select(
        "s.id",
        "s.end_date",
        "s.status",
        "s.claim_id",
        "s.claim_approved_amount",
        "s.claim_status_prefix",
        "s.end_date_month",
        "s.premium_plus_claim_approved_amount",
        "s.claim_approved_amount_avg_2_prior",
        "s.next2_end_date",
        "s.claim_approved_amount_quintile",
        "s.claim_adjuster_id",
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    joined
    .select(
        "id",
        "status",
        "claim_approved_amount",
        "end_date_month",
        "premium_plus_claim_approved_amount",
        "next2_end_date",
        "claim_approved_amount_quintile",
        "claim_adjuster_id",
        "adjuster_team",
        F.round(F.col("claim_approved_amount") * 0.25, 2).alias("claim_approved_amount_adj"),
        F.concat(F.col("adjuster_team"), F.lit("#"), F.col("id").cast("string")).alias("adjuster_team_ref"),
    )
    .orderBy("claim_adjuster_id")
)
