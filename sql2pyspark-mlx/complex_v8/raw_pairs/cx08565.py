from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id", "adjuster_team").orderBy(F.col("policy_end_date_plus_30d").desc(), F.col("id").desc())

base_claims = (
    df_claims
    .filter(F.col("policy_id") <= 23)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

combined_details = (
    base_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

combined_details_2 = (
    combined_details.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

scored = (
    combined_details_2
    .withColumn("policy_end_date_plus_30d", F.date_add(F.col("policy_end_date"), 30))
)

with_details_policy = (
    scored.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.policy_status",
        "s.policy_end_date",
        "s.adjuster_name",
        "s.adjuster_level",
        "s.adjuster_team",
        "s.policy_end_date_plus_30d",
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("a.team").alias("adjuster_team_2"),
    )
)

latest = (
    with_details_policy
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "policy_end_date",
        "adjuster_name",
        "adjuster_level",
        "adjuster_team",
        "policy_end_date_plus_30d",
        "policy_product",
        "policy_start_date",
        "adjuster_team_2",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
    .orderBy(F.col("policy_start_date").asc())
)
