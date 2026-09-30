from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_prior_rows_60d = Window.partitionBy("policy_id").orderBy("prior_rows_60d_plus_prior_claimed_amount_60d", "id").rowsBetween(-1, -1)
w_policy_id_by_prior_rows_60d_2 = Window.partitionBy("policy_id").orderBy("prior_rows_60d_plus_prior_claimed_amount_60d", "id").rowsBetween(-2, Window.currentRow)
w_prior_rows_60d = Window.partitionBy("prior_rows_60d")
w_policy_id_by_prior_rows_60d_3 = Window.partitionBy("policy_id").orderBy("prior_rows_60d_plus_prior_claimed_amount_60d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_prior_rows_60d_by = Window.partitionBy("policy_id", "prior_rows_60d").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_prior_rows_60d_4 = Window.partitionBy("policy_id").orderBy("prior_rows_60d_plus_prior_claimed_amount_60d", "id")

joined_ranges = (
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

annotated = (
    joined_ranges
    .filter(F.col("prior_rows_60d") > 1)
    .withColumn("prior_rows_60d_plus_prior_claimed_amount_60d", F.col("prior_rows_60d") + F.col("prior_claimed_amount_60d"))
)

with_trends = (
    annotated
    .select(
        "id",
        "policy_id",
        "prior_rows_60d",
        "prior_claimed_amount_60d",
        F.round(F.avg("prior_rows_60d").over(w_policy_id_by_prior_rows_60d), 2).alias("prior_rows_60d_avg_1_prior"),
        F.sum("prior_rows_60d").over(w_policy_id_by_prior_rows_60d_2).alias("prior_rows_60d_sum_3"),
        F.round(F.col("prior_rows_60d") / F.nullif(F.sum("prior_rows_60d").over(w_prior_rows_60d), F.lit(0)), 4).alias("prior_rows_60d_share_of_prior_rows_60d"),
        F.nth_value("claim_date", 2).over(w_policy_id_by_prior_rows_60d_3).alias("second_claim_date"),
        F.first("prior_claimed_amount_60d", ignorenulls=True).over(w_policy_id_prior_rows_60d_by).alias("first_known_prior_claimed_amount_60d"),
        F.row_number().over(w_policy_id_by_prior_rows_60d_4).alias("seq_no"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "policy_id",
        "prior_rows_60d",
        "prior_claimed_amount_60d",
        "prior_rows_60d_avg_1_prior",
        "prior_rows_60d_sum_3",
        "prior_rows_60d_share_of_prior_rows_60d",
        "first_known_prior_claimed_amount_60d",
        "seq_no",
    )
)
