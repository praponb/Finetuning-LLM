from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_all_by_prior_rows_180d_ties = Window.orderBy(F.col("prior_rows_180d").desc())
w_all_by_prior_rows_180d = Window.orderBy("prior_rows_180d", "id")

in_window = (
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

windowed = (
    in_window
    .select(
        "*",
        F.round(F.col("prior_claimed_amount_180d") - F.avg("prior_claimed_amount_180d").over(w_policy_id), 2).alias("prior_claimed_amount_180d_vs_policy_id_avg"),
        F.round(F.col("prior_claimed_amount_180d") / F.nullif(F.sum("prior_claimed_amount_180d").over(w_policy_id), F.lit(0)), 4).alias("prior_claimed_amount_180d_share_of_policy_id"),
        F.round(F.avg("prior_rows_180d").over(w_policy_id), 2).alias("policy_id_avg_prior_rows_180d"),
        F.round(F.cume_dist().over(w_all_by_prior_rows_180d_ties), 3).alias("prior_rows_180d_cume_dist"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_prior_rows_180d)).alias("days_since_prev_claim_date"),
    )
)

with_activity = (
    windowed.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
)

without_matches = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date").isNotNull()), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_180d",
        "prior_claimed_amount_180d",
        "prior_claimed_amount_180d_vs_policy_id_avg",
        "prior_claimed_amount_180d_share_of_policy_id",
        "policy_id_avg_prior_rows_180d",
        "prior_rows_180d_cume_dist",
        "days_since_prev_claim_date",
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
    )
    .orderBy(
        F.col("policy_id_avg_prior_rows_180d").desc(),
        F.col("claim_date").desc_nulls_first(),
    )
)
