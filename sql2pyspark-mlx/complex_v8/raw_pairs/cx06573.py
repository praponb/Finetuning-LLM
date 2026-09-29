from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy(F.col("claimed_amount").desc())
w_claim_type_by_claimed_amount_2 = Window.partitionBy("claim_type").orderBy(F.col("claimed_amount").desc(), "id")
w_claim_type_adjuster_id_by_cl = Window.partitionBy("claim_type", "adjuster_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_adjuster_id_by_cl_2 = Window.partitionBy("claim_type", "adjuster_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy("reported_at", "id")
w_claim_type_by_reported_at = Window.partitionBy("claim_type").orderBy("reported_at", "id")
w_policy_id_by_reported_at_mov = Window.partitionBy("policy_id").orderBy("reported_at", "id").rowsBetween(-3, Window.currentRow)
w_claim_type_claimed_amount_de = Window.partitionBy("claim_type", "claimed_amount_dense_rank")

filtered_claims = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

sequenced = (
    filtered_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.percent_rank().over(w_claim_type_by_claimed_amount), 3).alias("claimed_amount_percent_rank"),
        F.dense_rank().over(w_claim_type_by_claimed_amount_2).alias("claimed_amount_dense_rank"),
        F.last("claimed_amount", ignorenulls=True).over(w_claim_type_adjuster_id_by_cl).alias("last_known_claimed_amount"),
        F.lag("claimed_amount").over(w_claim_type_adjuster_id_by_cl_2).alias("prev_claimed_amount"),
    )
)

with_windows = (
    sequenced
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "claimed_amount_percent_rank",
        "claimed_amount_dense_rank",
        "last_known_claimed_amount",
        "prev_claimed_amount",
        (F.col("claimed_amount_dense_rank") - F.lag("claimed_amount_dense_rank").over(w_policy_id_by_reported_at)).alias("claimed_amount_dense_rank_change"),
        F.ntile(10).over(w_claim_type_by_reported_at).alias("reported_at_decile"),
        F.sum("claimed_amount_dense_rank").over(w_policy_id_by_reported_at_mov).alias("claimed_amount_dense_rank_sum_4"),
        F.lead("prev_claimed_amount").over(w_policy_id_by_reported_at).alias("next_prev_claimed_amount"),
        F.lead("claimed_amount_percent_rank").over(w_policy_id_by_reported_at).alias("next_claimed_amount_percent_rank"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type_claimed_amount_de), 2).alias("approved_amount_vs_claim_type_claimed_amount_den"),
    )
)

with_children_claims = (
    with_windows.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.claimed_amount_percent_rank",
        "s.prev_claimed_amount",
        "s.reported_at_decile",
        "s.next_prev_claimed_amount",
        "s.next_claimed_amount_percent_rank",
        "s.approved_amount_vs_claim_type_claimed_amount_den",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

with_details = (
    with_children_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .filter(~F.col("c.status").isin("PAID", "OPEN"))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.claimed_amount_percent_rank",
        "s.prev_claimed_amount",
        "s.reported_at_decile",
        "s.next_prev_claimed_amount",
        "s.next_claimed_amount_percent_rank",
        "s.approved_amount_vs_claim_type_claimed_amount_den",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_approved_amount",
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
    )
)

combined_details_claim = (
    with_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.claimed_amount_percent_rank",
        "s.prev_claimed_amount",
        "s.reported_at_decile",
        "s.next_prev_claimed_amount",
        "s.next_claimed_amount_percent_rank",
        "s.approved_amount_vs_claim_type_claimed_amount_den",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_reported_at",
        "s.claim_status",
        F.col("c.adjuster_id").alias("claim_adjuster_id_2"),
    )
)

per_group = (
    combined_details_claim
    .groupBy("claim_status")
    .agg(
        F.round(100 * F.count(F.when(F.col("claim_status") != "OPEN", 1)) / F.count("*"), 1).alias("pct_claim_status_open"),
        F.round(F.stddev("prev_claimed_amount"), 2).alias("stddev_prev_claimed_amount"),
        F.min("next_prev_claimed_amount").alias("min_next_prev_claimed_amount"),
        F.count("*").alias("cnt"),
        F.min("claim_reported_at").alias("first_claim_reported_at"),
    )
)

result = (
    per_group
    .groupBy("cnt", "first_claim_reported_at")
    .pivot("claim_status", ["OPEN", "APPROVED"])
    .agg(F.max("pct_claim_status_open"))
    .orderBy("cnt", "first_claim_reported_at")
)
