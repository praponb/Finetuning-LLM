from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_id").orderBy("claim_date", "id")
w_prior_rows_90d_by_prior_clai = Window.partitionBy("prior_rows_90d").orderBy("prior_claimed_amount_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_date_year = Window.partitionBy("policy_id", F.year("claim_date"))
w_policy_id_claim_date_month_b = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date")).orderBy(F.col("prior_rows_90d").asc_nulls_last())

joined_ranges = (
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

latest = (
    joined_ranges
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest
    .select(
        "*",
        F.first("claim_date", ignorenulls=True).over(w_prior_rows_90d_by_prior_clai).alias("first_known_claim_date"),
        F.max("prior_rows_90d").over(w_policy_id_claim_date_year).alias("policy_id_claim_date_year_max_prior_rows_90d"),
        F.rank().over(w_policy_id_claim_date_month_b).alias("prior_rows_90d_rank"),
        F.max("prior_claimed_amount_90d").over(w_policy_id_claim_date_year).alias("policy_id_claim_date_year_max_prior_claimed_amou"),
        F.count(F.lit(1)).over(w_policy_id_claim_date_year).alias("rows_in_policy_id_claim_date_year"),
        F.round(F.col("prior_rows_90d") / F.nullif(F.sum("prior_rows_90d").over(w_policy_id_claim_date_year), F.lit(0)), 4).alias("prior_rows_90d_share_of_policy_id_claim_date_yea"),
    )
)

result = (
    ranked
    .select(
        "id",
        "prior_rows_90d_rank",
        "policy_id_claim_date_year_max_prior_rows_90d",
    )
    .unpivot(
        ids=["id"],
        values=["prior_rows_90d_rank", "policy_id_claim_date_year_max_prior_rows_90d"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric_name")
)
