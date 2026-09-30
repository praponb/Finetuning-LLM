from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_premium_ties = Window.partitionBy("product").orderBy(F.col("premium").desc())
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_product_by_premium_prior_1 = Window.partitionBy("product").orderBy(F.col("premium").desc(), "id").rowsBetween(-1, -1)
w_product_by_premium = Window.partitionBy("product").orderBy(F.col("premium").desc(), "id")

covered = (
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

metrics = (
    covered
    .select(
        "*",
        F.round(F.cume_dist().over(w_product_by_premium_ties), 3).alias("premium_cume_dist"),
        F.round(F.avg("approved_amount").over(w_adjuster_id_policy_id), 2).alias("adjuster_id_policy_id_avg_approved_amount"),
        F.round(F.avg("approved_amount").over(w_product_by_premium_prior_1), 2).alias("approved_amount_avg_1_prior"),
        F.count(F.lit(1)).over(w_adjuster_id_policy_id).alias("rows_in_adjuster_id_policy_id"),
        F.round(F.percent_rank().over(w_product_by_premium), 3).alias("premium_percent_rank"),
        F.sum("premium").over(w_adjuster_id_policy_id).alias("adjuster_id_policy_id_sum_premium"),
    )
)

screened = (
    metrics.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id") >= 13) & (F.hour(F.col("c.reported_at")).between(8, 12)), "left_semi")
)

with_details_policy = (
    screened.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.adjuster_id",
        "s.approved_amount",
        "s.product",
        "s.premium",
        "s.premium_cume_dist",
        "s.adjuster_id_policy_id_avg_approved_amount",
        "s.approved_amount_avg_1_prior",
        "s.rows_in_adjuster_id_policy_id",
        "s.premium_percent_rank",
        "s.adjuster_id_policy_id_sum_premium",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("po.joined_on").alias("holder_joined_on"),
        "po.holder_name",
    )
)

result = (
    with_details_policy
    .groupBy("adjuster_id", "policy_id")
    .pivot("product", ["LIFE", "HEALTH", "TRAVEL"])
    .agg(F.avg("rows_in_adjuster_id_policy_id"))
)
