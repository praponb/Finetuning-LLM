from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_prior_rows_6 = Window.partitionBy(F.year("claim_date"), "prior_rows_60d").orderBy(F.col("prior_rows_60d").asc_nulls_last(), "id")
w_claim_date_year_prior_rows_6_2 = Window.partitionBy(F.year("claim_date"), "prior_rows_60d").orderBy(F.col("prior_rows_60d").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prior_rows_60d = Window.partitionBy("prior_rows_60d")
w_policy_id_prior_rows_60d_by = Window.partitionBy("policy_id", "prior_rows_60d").orderBy("claim_date", "id")
w_claim_date_year_prior_rows_6_3 = Window.partitionBy(F.year("claim_date"), "prior_rows_60d").orderBy(F.col("prior_rows_60d").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

ranked = (
    in_window
    .select(
        "*",
        F.lead("prior_claimed_amount_60d", 2, 0).over(w_claim_date_year_prior_rows_6).alias("next2_prior_claimed_amount_60d"),
        F.last("prior_claimed_amount_60d").over(w_claim_date_year_prior_rows_6_2).alias("last_prior_claimed_amount_60d"),
        F.size(F.collect_set("policy_id").over(w_prior_rows_60d)).alias("distinct_policy_id_in_prior_rows_60d"),
        F.row_number().over(w_policy_id_prior_rows_60d_by).alias("seq_no"),
        F.count(F.lit(1)).over(w_claim_date_year_prior_rows_6_3).alias("running_count"),
    )
)

result = (
    ranked
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_60d",
        "prior_claimed_amount_60d",
        "next2_prior_claimed_amount_60d",
        "last_prior_claimed_amount_60d",
        "running_count",
    )
    .orderBy(F.col("prior_claimed_amount_60d").desc())
)
