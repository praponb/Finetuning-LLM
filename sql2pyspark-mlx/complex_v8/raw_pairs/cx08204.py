from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type_by_repo = Window.partitionBy("policy_id", "claim_type").orderBy("reported_at", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_reported_at = Window.orderBy(F.col("reported_at").desc(), "id")
w_claim_type_adjuster_id_by_re = Window.partitionBy("claim_type", "adjuster_id").orderBy("reported_at", "id")
w_seq_no = Window.partitionBy("seq_no")
w_seq_no_reported_at_seq_by_re = Window.partitionBy("seq_no", "reported_at_seq").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_seq_no_reported_at_seq_by_re_2 = Window.partitionBy("seq_no", "reported_at_seq").orderBy(F.col("reported_at").desc(), "id")

base = (
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
    )
)

with_parent_policy = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.reported_at",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

with_activity = (
    with_parent_policy.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

metrics = (
    with_activity
    .select(
        "*",
        F.row_number().over(w_policy_id_claim_type_by_repo).alias("seq_no"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        F.row_number().over(w_all_by_reported_at).alias("reported_at_seq"),
        F.row_number().over(w_claim_type_adjuster_id_by_re).alias("reported_at_seq_2"),
        F.round(F.cume_dist().over(w_claim_type_adjuster_id_by_re), 3).alias("reported_at_cume_dist"),
        F.max("claimed_amount").over(w_adjuster_id).alias("adjuster_id_max_claimed_amount"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.count(F.lit(1)).over(w_seq_no).alias("rows_in_seq_no"),
        F.sum("reported_at_seq").over(w_seq_no).alias("seq_no_sum_reported_at_seq"),
        F.last("seq_no", ignorenulls=True).over(w_seq_no_reported_at_seq_by_re).alias("last_known_seq_no"),
        F.max("reported_at_cume_dist").over(w_seq_no).alias("seq_no_max_reported_at_cume_dist"),
        (F.col("reported_at_seq") - F.lag("reported_at_seq").over(w_seq_no_reported_at_seq_by_re_2)).alias("reported_at_seq_change"),
    )
)

result = (
    metrics_2
    .filter(F.col("reported_at_cume_dist") <= 0.8)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "policy_start_date",
        "distinct_policy_id_in_adjuster_id",
        "reported_at_seq_2",
        "reported_at_cume_dist",
        "adjuster_id_max_claimed_amount",
        "rows_in_seq_no",
        "reported_at_seq_change",
    )
    .orderBy(
        F.col("reported_at_seq_change").desc(),
        F.col("reported_at_cume_dist").desc(),
        "id",
    )
    .limit(5)
)
