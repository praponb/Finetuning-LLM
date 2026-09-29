from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_policy_status = Window.partitionBy("policy_id", "policy_status")

in_window = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 30)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_30d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_30d"),
    )
)

with_parent_policy = (
    in_window.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_30d",
        "s.prior_claimed_amount_30d",
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
    )
)

sequenced = (
    with_parent_policy
    .select(
        "id",
        "prior_claimed_amount_30d",
        "policy_status",
        "policy_product",
        F.max("prior_rows_30d").over(w_policy_id_policy_status).alias("policy_id_policy_status_max_prior_rows_30d"),
        F.round(F.col("prior_claimed_amount_30d") - F.avg("prior_claimed_amount_30d").over(w_policy_id_policy_status), 2).alias("prior_claimed_amount_30d_vs_policy_id_policy_sta"),
    )
)

with_activity = (
    sequenced
    .crossJoin(sequenced.agg(F.min("prior_claimed_amount_30d_vs_policy_id_policy_sta").alias("overall_min_prior_claimed_amount_30d_vs_policy_i")))
    .filter(F.col("prior_claimed_amount_30d_vs_policy_id_policy_sta") > F.col("overall_min_prior_claimed_amount_30d_vs_policy_i"))
    .drop("overall_min_prior_claimed_amount_30d_vs_policy_i")
)

result = (
    with_activity
    .select(
        "id",
        "policy_status",
        "policy_product",
        "policy_id_policy_status_max_prior_rows_30d",
        "prior_claimed_amount_30d_vs_policy_id_policy_sta",
    )
    .orderBy(F.col("policy_status").asc(), F.col("policy_product").desc(), "id")
    .limit(20)
)
