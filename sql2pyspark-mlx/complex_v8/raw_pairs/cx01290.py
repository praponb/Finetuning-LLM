from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_last_claim_date = Window.orderBy(F.col("last_claim_date").desc_nulls_first(), "policy_id")
w_all_by_last_claim_date_full = Window.orderBy(F.col("last_claim_date").desc_nulls_first(), "policy_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cnt_policy_id = Window.partitionBy("cnt", "policy_id")
w_all_by_last_last_claim_date = Window.orderBy("last_last_claim_date", "policy_id")
w_cnt_by_cnt_prior_1 = Window.partitionBy("cnt").orderBy(F.col("cnt").desc(), "policy_id").rowsBetween(-1, -1)

covered = (
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
        F.sum("b.approved_amount").alias("prior_approved_amount_180d"),
    )
)

stats = (
    covered
    .groupBy("policy_id")
    .agg(
        F.round(F.sum("prior_approved_amount_180d") / F.nullif(F.sum("prior_rows_180d"), F.lit(0)), 3).alias("prior_approved_amount_180d_to_prior_rows_180d_ra"),
        F.max("claim_date").alias("last_claim_date"),
        F.count("*").alias("cnt"),
    )
)

sequenced = (
    stats
    .select(
        "policy_id",
        "last_claim_date",
        "cnt",
        F.dense_rank().over(w_all_by_last_claim_date).alias("last_claim_date_dense_rank"),
        F.last("last_claim_date").over(w_all_by_last_claim_date_full).alias("last_last_claim_date"),
        F.row_number().over(w_all_by_last_claim_date).alias("last_claim_date_seq"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.min("last_claim_date_seq").over(w_cnt_policy_id).alias("cnt_policy_id_min_last_claim_date_seq"),
        F.rank().over(w_all_by_last_last_claim_date).alias("last_last_claim_date_rank"),
        F.lag("last_last_claim_date").over(w_all_by_last_last_claim_date).alias("prev_last_last_claim_date"),
        F.sum("cnt").over(w_cnt_by_cnt_prior_1).alias("cnt_sum_1_prior"),
    )
)

result = (
    with_trends
    .select(
        "policy_id",
        "last_claim_date",
        "last_claim_date_dense_rank",
        "last_last_claim_date",
        "prev_last_last_claim_date",
        "cnt_sum_1_prior",
        F.datediff(F.col("last_last_claim_date"), F.col("last_claim_date")).alias("days_last_claim_date_to_last_last_claim_date"),
        F.least(F.col("cnt"), F.col("last_claim_date_seq")).alias("least_cnt_last_claim_date_seq"),
    )
    .orderBy(F.col("least_cnt_last_claim_date_seq").desc_nulls_first())
)
