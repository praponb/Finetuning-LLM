from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_status_len_adjuster_id_by_cl = Window.partitionBy("status_len", "adjuster_id").orderBy("claimed_amount_per_approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_claims = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2025-10-29").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.date_trunc("month", F.col("reported_at")).alias("reported_at_month"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

sequenced = (
    prepared_claims
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "status_len",
        "reported_at_month",
        "claimed_amount_per_approved_amount",
        F.sum("status_len").over(w_adjuster_id_policy_id).alias("adjuster_id_policy_id_sum_status_len"),
        F.round(F.avg("approved_amount").over(w_adjuster_id_policy_id), 2).alias("adjuster_id_policy_id_avg_approved_amount"),
        F.last("status_len", ignorenulls=True).over(w_status_len_adjuster_id_by_cl).alias("last_known_status_len"),
    )
)

joined = (
    sequenced.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.lower(F.col("a.adjuster_name")).like("%va%"))
    .select("s.*", "a.adjuster_name", F.col("a.team").alias("adjuster_team"))
)

with_children = (
    joined.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_date").isNotNull()), "left")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at_month",
        "s.adjuster_id_policy_id_avg_approved_amount",
        "s.adjuster_name",
        "s.adjuster_team",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_type",
        F.col("c.status").alias("claim_status"),
        "c.claim_date",
    )
)

result = (
    with_children
    .select(
        "id",
        "claim_id",
        "adjuster_id_policy_id_avg_approved_amount",
        "approved_amount",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["adjuster_id_policy_id_avg_approved_amount", "approved_amount"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "claim_id", "field")
)
