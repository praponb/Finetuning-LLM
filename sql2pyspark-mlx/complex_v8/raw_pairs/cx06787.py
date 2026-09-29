from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_by_claim_dat = Window.partitionBy(F.year("claim_date")).orderBy(F.col("claim_date").desc(), "id")
w_claim_date_year = Window.partitionBy(F.year("claim_date"))
w_claim_date_year_by_claim_dat_2 = Window.partitionBy(F.year("claim_date")).orderBy(F.col("claim_date").desc(), "id").rowsBetween(-6, -1)

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

policy_id_stats = (
    covered
    .groupBy("policy_id")
    .agg(F.avg("prior_rows_180d").alias("avg_prior_rows_180d"))
)

above_average = (
    covered.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.prior_rows_180d") > F.col("g.avg_prior_rows_180d"))
    .select(
        "s.*",
        "g.avg_prior_rows_180d",
        F.round(F.col("s.prior_rows_180d") - F.col("g.avg_prior_rows_180d"), 2).alias("prior_rows_180d_minus_avg"),
    )
)

ranked = (
    above_average
    .select(
        "*",
        F.ntile(4).over(w_claim_date_year_by_claim_dat).alias("claim_date_quartile"),
        F.min("prior_rows_180d_minus_avg").over(w_claim_date_year).alias("claim_date_year_min_prior_rows_180d_minus_avg"),
        F.ntile(10).over(w_claim_date_year_by_claim_dat).alias("claim_date_decile"),
        F.round(F.avg("prior_rows_180d").over(w_claim_date_year_by_claim_dat_2), 2).alias("prior_rows_180d_avg_6_prior"),
        F.sum("prior_rows_180d").over(w_claim_date_year).alias("claim_date_year_sum_prior_rows_180d"),
    )
)

result = (
    ranked
    .filter(F.col("prior_approved_amount_180d") < 225000.0)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_180d",
        "avg_prior_rows_180d",
        "claim_date_year_min_prior_rows_180d_minus_avg",
        "claim_date_decile",
        "prior_rows_180d_avg_6_prior",
        "claim_date_year_sum_prior_rows_180d",
    )
    .orderBy(
        "claim_date_year_sum_prior_rows_180d",
        "claim_date_year_min_prior_rows_180d_minus_avg",
        "id",
    )
    .limit(20)
)
