from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id_by_cla = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc(), "id")

in_window = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

with_trends = (
    in_window
    .select(
        "*",
        F.first("claimed_amount").over(w_adjuster_id_policy_id_by_cla).alias("first_claimed_amount"),
        F.min("claimed_amount").over(w_adjuster_id).alias("adjuster_id_min_claimed_amount"),
        F.max("approved_amount").over(w_adjuster_id).alias("adjuster_id_max_approved_amount"),
        F.row_number().over(w_policy_id_by_claim_date).alias("seq_no"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "policy_id",
        "claim_date",
        "adjuster_id",
        "approved_amount",
        "product",
        "adjuster_id_min_claimed_amount",
        "adjuster_id_max_approved_amount",
    )
    .orderBy("id")
)
