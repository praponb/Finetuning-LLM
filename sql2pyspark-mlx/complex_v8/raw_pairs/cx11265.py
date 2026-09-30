from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_status_by_approved_am = Window.partitionBy("policy_status").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-2, -1)
w_policy_status = Window.partitionBy("policy_status")

scoped = (
    df_claims
    .filter(F.col("approved_amount") > 2887.3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.date_format(F.col("reported_at"), "yyyy-MM").alias("reported_at_label"),
        F.date_format(F.col("reported_at"), "yyyy-MM").alias("reported_at_label_2"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level").between(2, 3)), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.round(F.avg("approved_amount").over(w_policy_status_by_approved_am), 2).alias("approved_amount_avg_2_prior"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_status), F.lit(0)), 4).alias("approved_amount_share_of_policy_status"),
    )
)

tagged = (
    windowed
    .select(
        "*",
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
        F.least(F.col("policy_premium"), F.col("approved_amount")).alias("least_policy_premium_approved_amount"),
    )
)

with_activity = (
    tagged
    .crossJoin(tagged.agg(F.avg("approved_amount_share_of_policy_status").alias("overall_avg_approved_amount_share_of_policy_stat")))
    .filter(F.col("approved_amount_share_of_policy_status") > F.col("overall_avg_approved_amount_share_of_policy_stat"))
    .drop("overall_avg_approved_amount_share_of_policy_stat")
)

screened = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

joined_policy = (
    screened.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select("s.*", F.col("p.holder_id").alias("policy_holder_id"))
)

result = (
    joined_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        "reported_at_label",
        "reported_at_label_2",
        "status_slug",
        "adjuster_team",
        "policy_premium",
        "policy_status",
        "approved_amount_avg_2_prior",
        "status_ref",
        "least_policy_premium_approved_amount",
        (F.col("policy_premium") - F.col("approved_amount")).alias("policy_premium_minus_approved_amount"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
    .orderBy(F.col("status_slug").desc(), F.col("claim_date").desc())
)
