from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_90d_policy_id_by = Window.partitionBy("prior_rows_90d", "policy_id").orderBy("claim_date")
w_policy_id_prior_rows_90d = Window.partitionBy("policy_id", "prior_rows_90d")
w_policy_id_by_prior_claimed_a = Window.partitionBy("policy_id").orderBy(F.col("prior_claimed_amount_90d").desc(), "id")
w_policy_id_by_prior_claimed_a_2 = Window.partitionBy("policy_id").orderBy(F.col("prior_claimed_amount_90d").desc())

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

windowed = (
    in_window
    .select(
        "*",
        F.round(F.percent_rank().over(w_prior_rows_90d_policy_id_by), 3).alias("claim_date_percent_rank"),
        F.count(F.lit(1)).over(w_policy_id_prior_rows_90d).alias("rows_in_policy_id_prior_rows_90d"),
        F.row_number().over(w_policy_id_by_prior_claimed_a).alias("seq_no"),
        F.rank().over(w_policy_id_by_prior_claimed_a_2).alias("prior_claimed_amount_90d_rank"),
        F.ntile(4).over(w_policy_id_by_prior_claimed_a).alias("prior_claimed_amount_90d_quartile"),
        F.round(F.col("prior_rows_90d") / F.nullif(F.sum("prior_rows_90d").over(w_policy_id_prior_rows_90d), F.lit(0)), 4).alias("prior_rows_90d_share_of_policy_id_prior_rows_90d"),
    )
)

top_rows = windowed.filter(F.col("prior_claimed_amount_90d_rank") == 1)

result = (
    top_rows
    .select(
        "id",
        "claim_date",
        "prior_claimed_amount_90d",
        "claim_date_percent_rank",
        "rows_in_policy_id_prior_rows_90d",
        "seq_no",
        "prior_claimed_amount_90d_rank",
        "prior_claimed_amount_90d_quartile",
        "prior_rows_90d_share_of_policy_id_prior_rows_90d",
        F.abs(F.col("seq_no") - 7).alias("seq_no_distance"),
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
    )
)
