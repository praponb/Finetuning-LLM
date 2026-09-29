from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_prior_2 = Window.orderBy("reported_at", "id").rowsBetween(-2, -1)
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at_ties = Window.orderBy("reported_at")

scoped = (
    df_claims
    .filter(F.col("approved_amount") < 22250.6)
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
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

filtered = (
    scoped.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount") < 32796.4), "left_semi")
)

metrics = (
    filtered
    .select(
        "*",
        F.max("approved_amount").over(w_all_by_reported_at_prior_2).alias("approved_amount_max_2_prior"),
        F.min("approved_amount").over(w_all_by_reported_at_running).alias("min_approved_amount_so_far"),
        F.round(F.cume_dist().over(w_all_by_reported_at_ties), 3).alias("reported_at_cume_dist"),
    )
)

result = (
    metrics
    .filter(F.col("status").isin("APPROVED", "REJECTED", "PAID"))
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "reported_at_date",
        "status_slug",
        "status_or_unknown",
        "approved_amount_max_2_prior",
        "min_approved_amount_so_far",
        "reported_at_cume_dist",
    )
    .orderBy(F.col("approved_amount").desc())
)
