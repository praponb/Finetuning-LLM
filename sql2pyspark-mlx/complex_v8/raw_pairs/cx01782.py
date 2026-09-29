from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_approved_amount = Window.partitionBy("status").orderBy(F.col("approved_amount").desc(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_status_by_approved_amount_fu = Window.partitionBy("status").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_claim_date = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), "id")

base_claims = (
    df_claims
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
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

sequenced = (
    base_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status_clean",
        F.ntile(10).over(w_status_by_approved_amount).alias("approved_amount_decile"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id), 2).alias("approved_amount_vs_policy_id_avg"),
        F.nth_value("claim_date", 2).over(w_status_by_approved_amount_fu).alias("second_claim_date"),
        F.ntile(5).over(w_adjuster_id_by_claim_date).alias("claim_date_quintile"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team") != "Health") & (F.upper(F.trim(F.col("a.adjuster_name"))).like("F%")), "left_semi")
)

result = (
    qualified
    .groupBy("policy_id", "claim_date")
    .pivot("claim_type", ["medical", "flood", "delay"])
    .agg(F.count("approved_amount_decile"))
)
