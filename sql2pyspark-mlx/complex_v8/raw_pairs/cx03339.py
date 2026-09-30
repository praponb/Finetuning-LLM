from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_adjuster_id_claim_date = Window.partitionBy("claim_adjuster_id", F.year("claim_date"))
w_all_by_claim_date_full = Window.orderBy("claim_date", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_holder_risk_score = Window.orderBy("holder_risk_score", "id", "claim_id")

cleaned = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.abs(F.col("premium") - 3465.4).alias("premium_distance"),
    )
)

detail_rows_claims = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium_filled",
        "s.premium_distance",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_date",
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

combined_details = (
    detail_rows_claims.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium_filled",
        "s.premium_distance",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_date",
        "s.claim_reported_at",
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.round(F.col("premium_distance") - F.avg("premium_distance").over(w_claim_adjuster_id_claim_date), 2).alias("premium_distance_vs_claim_adjuster_id_claim_date"),
        F.first("claim_date").over(w_all_by_claim_date_full).alias("first_claim_date"),
        F.ntile(10).over(w_all_by_holder_risk_score).alias("holder_risk_score_decile"),
    )
)

result = (
    windowed
    .filter(F.col("claim_adjuster_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium_filled",
        "premium_distance",
        "claim_id",
        "claim_date",
        "claim_reported_at",
        "holder_name",
        "holder_joined_on",
        "holder_risk_score",
        "premium_distance_vs_claim_adjuster_id_claim_date",
        "holder_risk_score_decile",
    )
)
