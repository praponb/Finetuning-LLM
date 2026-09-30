from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_90d = Window.partitionBy("prior_rows_90d")
w_policy_id_by_claim_date_prio = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(-6, -1)
w_all_by_prior_rows_90d_full = Window.orderBy("prior_rows_90d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

in_window = (
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

ranked = (
    in_window
    .select(
        "*",
        F.round(F.col("prior_rows_90d") / F.nullif(F.sum("prior_rows_90d").over(w_prior_rows_90d), F.lit(0)), 4).alias("prior_rows_90d_share_of_prior_rows_90d"),
        F.round(F.avg("prior_claimed_amount_90d").over(w_prior_rows_90d), 2).alias("prior_rows_90d_avg_prior_claimed_amount_90d"),
        F.round(F.col("prior_claimed_amount_90d") / F.nullif(F.sum("prior_claimed_amount_90d").over(w_prior_rows_90d), F.lit(0)), 4).alias("prior_claimed_amount_90d_share_of_prior_rows_90d"),
        F.max("prior_claimed_amount_90d").over(w_policy_id_by_claim_date_prio).alias("prior_claimed_amount_90d_max_6_prior"),
        F.nth_value("prior_rows_90d", 2).over(w_all_by_prior_rows_90d_full).alias("second_prior_rows_90d"),
    )
)

tagged = (
    ranked
    .filter(F.col("prior_claimed_amount_90d_max_6_prior") >= 75000.0)
    .select(
        "*",
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label_2"),
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
    )
)

without_matches = (
    tagged
    .crossJoin(tagged.agg(F.min("second_prior_rows_90d").alias("overall_min_second_prior_rows_90d")))
    .filter(F.col("second_prior_rows_90d") > F.col("overall_min_second_prior_rows_90d"))
    .drop("overall_min_second_prior_rows_90d")
)

result = (
    without_matches
    .filter(F.col("prior_claimed_amount_90d_share_of_prior_rows_90d") > 0.3)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_90d",
        "prior_claimed_amount_90d",
        "prior_rows_90d_share_of_prior_rows_90d",
        "prior_rows_90d_avg_prior_claimed_amount_90d",
        "prior_claimed_amount_90d_share_of_prior_rows_90d",
        "claim_date_label",
        "claim_date_label_2",
        F.substring(F.trim(F.col("claim_date_label")), 1, 3).alias("claim_date_label_prefix"),
        F.round(F.col("prior_rows_90d") / F.nullif(F.col("second_prior_rows_90d"), F.lit(0)), 3).alias("prior_rows_90d_per_second_prior_rows_90d"),
    )
    .orderBy(
        F.col("prior_claimed_amount_90d_share_of_prior_rows_90d").asc_nulls_last(),
        F.col("claim_date_label").desc(),
    )
)
