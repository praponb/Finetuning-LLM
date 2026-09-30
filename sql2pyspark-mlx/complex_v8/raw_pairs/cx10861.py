from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_prior_rows_30d = Window.partitionBy("policy_id", "prior_rows_30d")
w_all_by_prior_rows_30d = Window.orderBy("prior_rows_30d", "id")
w_latest = Window.partitionBy("policy_id").orderBy("claim_date", "id")

covered = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 30)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_30d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_30d"),
    )
)

with_windows = (
    covered
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_30d",
        "prior_approved_amount_30d",
        F.round(F.col("prior_rows_30d") / F.nullif(F.sum("prior_rows_30d").over(w_policy_id_prior_rows_30d), F.lit(0)), 4).alias("prior_rows_30d_share_of_policy_id_prior_rows_30d"),
        F.round(F.percent_rank().over(w_all_by_prior_rows_30d), 3).alias("prior_rows_30d_percent_rank"),
    )
)

latest_per_key = (
    with_windows
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "policy_id",
        "prior_approved_amount_30d",
        "prior_rows_30d_share_of_policy_id_prior_rows_30d",
        "prior_rows_30d_percent_rank",
    )
    .orderBy("prior_rows_30d_share_of_policy_id_prior_rows_30d", "id")
    .limit(5)
)
