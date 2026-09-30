from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_start_date_year_by_pr = Window.partitionBy(F.year("policy_start_date")).orderBy("prior_rows_60d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_prior_claimed_amount = Window.orderBy("prior_claimed_amount_60d", "id")
w_policy_start_date_year_by_pr_2 = Window.partitionBy(F.year("policy_start_date")).orderBy("prior_rows_60d", "id")

matched = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 60)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_60d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_60d"),
    )
)

with_details_policy = (
    matched.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_60d",
        "s.prior_claimed_amount_60d",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

with_trends = (
    with_details_policy
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_60d",
        "prior_claimed_amount_60d",
        F.count(F.lit(1)).over(w_policy_start_date_year_by_pr).alias("running_count"),
        F.datediff(F.col("policy_start_date"), F.lag("policy_start_date").over(w_all_by_prior_claimed_amount)).alias("days_since_prev_policy_start_date"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_prior_claimed_amount)).alias("days_since_prev_claim_date"),
        F.ntile(10).over(w_policy_start_date_year_by_pr_2).alias("prior_rows_60d_decile"),
    )
)

tagged = (
    with_trends
    .select(
        "id",
        "claim_date",
        "prior_claimed_amount_60d",
        "days_since_prev_policy_start_date",
        "days_since_prev_claim_date",
        "prior_rows_60d_decile",
        (F.floor(F.col("prior_claimed_amount_60d") / 50000) * 50000).alias("prior_claimed_amount_60d_bucket"),
        F.abs(F.col("prior_claimed_amount_60d") - 175000.0).alias("prior_claimed_amount_60d_distance"),
        F.coalesce(F.col("prior_rows_60d_decile"), F.lit(0)).alias("prior_rows_60d_decile_filled"),
    )
)

result = (
    tagged
    .select(
        "id",
        "claim_date",
        "prior_claimed_amount_60d",
        "days_since_prev_policy_start_date",
        "days_since_prev_claim_date",
        "prior_rows_60d_decile",
        "prior_claimed_amount_60d_bucket",
        "prior_claimed_amount_60d_distance",
        F.coalesce(F.col("prior_claimed_amount_60d"), F.lit(0.0)).alias("prior_claimed_amount_60d_filled"),
    )
    .orderBy("days_since_prev_policy_start_date")
)
