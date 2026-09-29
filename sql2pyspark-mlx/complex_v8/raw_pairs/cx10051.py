from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month = Window.partitionBy(F.date_trunc("month", "claim_date"))
w_claim_date_month_policy_id_b = Window.partitionBy(F.date_trunc("month", "claim_date"), "policy_id").orderBy("least_prior_claimed_amount_60d_prior_rows_60d", "id")

in_window = (
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

enriched = (
    in_window
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.least(F.col("prior_claimed_amount_60d"), F.col("prior_rows_60d")).alias("least_prior_claimed_amount_60d_prior_rows_60d"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "policy_id",
        "prior_rows_60d",
        "prior_claimed_amount_60d",
        "least_prior_claimed_amount_60d_prior_rows_60d",
        F.round(F.col("prior_rows_60d") - F.avg("prior_rows_60d").over(w_claim_date_month), 2).alias("prior_rows_60d_vs_claim_date_month_avg"),
        F.ntile(3).over(w_claim_date_month_policy_id_b).alias("least_prior_claimed_amount_60d_prior_rows_60d_te"),
        F.round(F.col("prior_rows_60d") / F.nullif(F.sum("prior_rows_60d").over(w_claim_date_month), F.lit(0)), 4).alias("prior_rows_60d_share_of_claim_date_month"),
    )
)

with_parent = (
    windowed.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.prior_rows_60d",
        "s.prior_claimed_amount_60d",
        "s.least_prior_claimed_amount_60d_prior_rows_60d",
        "s.prior_rows_60d_vs_claim_date_month_avg",
        "s.least_prior_claimed_amount_60d_prior_rows_60d_te",
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    with_parent
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "prior_claimed_amount_60d",
        "least_prior_claimed_amount_60d_prior_rows_60d",
        "prior_rows_60d_vs_claim_date_month_avg",
        "least_prior_claimed_amount_60d_prior_rows_60d_te",
        F.coalesce(F.col("policy_product"), F.lit("UNKNOWN")).alias("policy_product_or_unknown"),
        F.coalesce(F.col("policy_product"), F.lit("UNKNOWN")).alias("policy_product_or_unknown_2"),
    )
    .orderBy(
        F.col("id").desc_nulls_first(),
        F.col("policy_product_or_unknown_2").desc(),
    )
)
