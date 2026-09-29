from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_180d_by_prior_cla = Window.partitionBy("prior_rows_180d").orderBy(F.col("prior_claimed_amount_180d").asc_nulls_last(), "id")
w_prior_rows_180d_by_prior_cla_2 = Window.partitionBy("prior_rows_180d").orderBy(F.col("prior_claimed_amount_180d").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_prior_rows_180d = Window.partitionBy("policy_id", "prior_rows_180d")
w_all_by_prior_claimed_amount = Window.orderBy("prior_claimed_amount_180d")
w_second_prior_rows_180d = Window.partitionBy("second_prior_rows_180d")

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

ranked = (
    matched
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_180d",
        "prior_claimed_amount_180d",
        (F.col("prior_rows_180d") - F.lag("prior_rows_180d").over(w_prior_rows_180d_by_prior_cla)).alias("prior_rows_180d_change"),
        F.nth_value("prior_rows_180d", 2).over(w_prior_rows_180d_by_prior_cla_2).alias("second_prior_rows_180d"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_prior_rows_180d_by_prior_cla)).alias("days_since_prev_claim_date"),
        F.max("prior_claimed_amount_180d").over(w_policy_id_prior_rows_180d).alias("policy_id_prior_rows_180d_max_prior_claimed_amou"),
    )
)

metrics = (
    ranked
    .select(
        "id",
        "policy_id",
        "prior_rows_180d",
        "prior_claimed_amount_180d",
        F.rank().over(w_all_by_prior_claimed_amount).alias("prior_claimed_amount_180d_rank"),
        F.count(F.lit(1)).over(w_second_prior_rows_180d).alias("rows_in_second_prior_rows_180d"),
    )
)

derived = (
    metrics
    .select(
        "id",
        "policy_id",
        "prior_rows_180d",
        "prior_claimed_amount_180d",
        "rows_in_second_prior_rows_180d",
        F.coalesce(F.col("prior_rows_180d"), F.lit(0)).alias("prior_rows_180d_filled"),
    )
)

result = (
    derived
    .select(
        "id",
        "policy_id",
        "prior_claimed_amount_180d",
        "prior_rows_180d_filled",
    )
    .orderBy("policy_id", "id")
    .limit(5)
)
