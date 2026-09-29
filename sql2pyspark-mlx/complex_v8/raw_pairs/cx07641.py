from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_prior_rows_90d_by = Window.partitionBy("policy_id", "prior_rows_90d").orderBy("prior_claimed_amount_90d", "id")
w_prior_rows_90d = Window.partitionBy("prior_rows_90d")
w_policy_id_prior_rows_90d_by_2 = Window.partitionBy("policy_id", "prior_rows_90d").orderBy("prior_claimed_amount_90d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

covered = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 90)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_90d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_90d"),
    )
)

windowed = (
    covered
    .select(
        "*",
        F.lag("claim_date").over(w_policy_id_prior_rows_90d_by).alias("prev_claim_date"),
        F.round(F.col("prior_rows_90d") - F.avg("prior_rows_90d").over(w_prior_rows_90d), 2).alias("prior_rows_90d_vs_prior_rows_90d_avg"),
        F.round(F.col("prior_claimed_amount_90d") / F.nullif(F.sum("prior_claimed_amount_90d").over(w_prior_rows_90d), F.lit(0)), 4).alias("prior_claimed_amount_90d_share_of_prior_rows_90d"),
        F.nth_value("prior_claimed_amount_90d", 2).over(w_policy_id_prior_rows_90d_by_2).alias("second_prior_claimed_amount_90d"),
    )
)

result = (
    windowed
    .select("id", "prior_claimed_amount_90d")
    .distinct()
    .orderBy(F.col("id").desc())
)
