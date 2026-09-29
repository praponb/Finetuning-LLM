from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_180d = Window.partitionBy("prior_rows_180d")
w_policy_id_by_prior_rows_180d = Window.partitionBy("policy_id").orderBy("prior_rows_180d", "id").rowsBetween(-1, -1)
w_policy_id_by_claim_date_ties = Window.partitionBy("policy_id").orderBy("claim_date")
w_policy_id_by_claim_date_runn = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 180)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_180d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_180d"),
    )
)

with_activity = (
    matched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
)

sequenced = (
    with_activity
    .select(
        "*",
        F.max("prior_rows_180d").over(w_prior_rows_180d).alias("prior_rows_180d_max_prior_rows_180d"),
        F.round(F.col("prior_rows_180d") - F.avg("prior_rows_180d").over(w_prior_rows_180d), 2).alias("prior_rows_180d_vs_prior_rows_180d_avg"),
        F.max("prior_rows_180d").over(w_policy_id_by_prior_rows_180d).alias("prior_rows_180d_max_1_prior"),
        F.round(F.cume_dist().over(w_policy_id_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.sum("prior_claimed_amount_180d").over(w_policy_id_by_claim_date_runn).alias("running_prior_claimed_amount_180d"),
    )
)

policy_id_stats = (
    sequenced
    .groupBy("policy_id")
    .agg(F.avg("prior_rows_180d").alias("avg_prior_rows_180d"))
)

vs_group = (
    sequenced.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.prior_rows_180d") > F.col("g.avg_prior_rows_180d"))
    .select(
        "s.*",
        "g.avg_prior_rows_180d",
        F.round(F.col("s.prior_rows_180d") - F.col("g.avg_prior_rows_180d"), 2).alias("prior_rows_180d_minus_avg"),
    )
)

result = (
    vs_group
    .filter(F.col("prior_rows_180d_max_1_prior") <= 14)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_180d",
        "prior_claimed_amount_180d",
        "prior_rows_180d_max_prior_rows_180d",
        "prior_rows_180d_vs_prior_rows_180d_avg",
        "prior_rows_180d_max_1_prior",
        "running_prior_claimed_amount_180d",
        "avg_prior_rows_180d",
        "prior_rows_180d_minus_avg",
    )
    .orderBy(F.col("prior_rows_180d_max_prior_rows_180d").desc())
)
