from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_60d = Window.partitionBy("prior_rows_60d")
w_all_by_policy_premium_runnin = Window.orderBy(F.col("policy_premium").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_policy_premium_ties = Window.orderBy(F.col("policy_premium").asc_nulls_last())
w_policy_start_date_month_by_p = Window.partitionBy(F.date_trunc("month", "policy_start_date")).orderBy("policy_start_date", "id")

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
        F.sum("b.approved_amount").alias("prior_approved_amount_60d"),
    )
)

with_details = (
    in_window.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_60d",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("po.region").alias("holder_region"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.max("prior_rows_60d").over(w_prior_rows_60d).alias("prior_rows_60d_max_prior_rows_60d"),
        F.min("prior_rows_60d").over(w_prior_rows_60d).alias("prior_rows_60d_min_prior_rows_60d"),
        F.last("prior_rows_60d", ignorenulls=True).over(w_all_by_policy_premium_runnin).alias("last_known_prior_rows_60d"),
        F.round(F.percent_rank().over(w_all_by_policy_premium_ties), 3).alias("policy_premium_percent_rank"),
        F.row_number().over(w_policy_start_date_month_by_p).alias("seq_no"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "claim_date",
        "prior_rows_60d",
        "policy_premium",
        "policy_start_date",
        "holder_region",
        "prior_rows_60d_max_prior_rows_60d",
        "prior_rows_60d_min_prior_rows_60d",
        "last_known_prior_rows_60d",
        "policy_premium_percent_rank",
        F.coalesce(F.col("holder_region"), F.lit("UNKNOWN")).alias("holder_region_or_unknown"),
        F.upper(F.trim(F.col("holder_region"))).alias("holder_region_clean"),
    )
    .orderBy("policy_premium", F.col("holder_region_or_unknown").desc_nulls_first())
)
