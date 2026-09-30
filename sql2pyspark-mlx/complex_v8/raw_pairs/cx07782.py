from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claim_date_full = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy("claim_date", "id")
w_policy_id_by_claim_date_runn = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_claims = (
    df_claims
    .filter(F.col("status").isin("IN_REVIEW", "REJECTED", "PAID"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
    )
)

ranked = (
    recent_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        F.first("claim_date").over(w_policy_id_by_claim_date_full).alias("first_claim_date"),
        F.last("claim_date").over(w_policy_id_by_claim_date_full).alias("last_claim_date"),
        F.row_number().over(w_policy_id_by_claim_date).alias("seq_no"),
        F.first("claim_date", ignorenulls=True).over(w_policy_id_by_claim_date_runn).alias("first_known_claim_date"),
    )
)

filtered = (
    ranked.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") >= 4868.7), "left_semi")
    .filter(F.col("claim_type").isin("delay", "theft"))
)

with_activity = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") < 23487.5), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "claim_date",
        "claim_type",
        "last_claim_date",
        "seq_no",
        F.coalesce(F.col("seq_no"), F.lit(0)).alias("seq_no_filled"),
    )
    .orderBy(F.col("seq_no_filled").asc_nulls_last())
)
