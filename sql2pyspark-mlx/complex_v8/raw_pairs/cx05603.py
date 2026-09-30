from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_by_claim_dat = Window.partitionBy(F.year("claim_date")).orderBy(F.col("claim_date").desc(), "id", "claim_id").rowsBetween(-6, Window.currentRow)
w_claim_date_year_policy_id = Window.partitionBy(F.year("claim_date"), "policy_id")
w_claim_date_year_by_claim_dat_2 = Window.partitionBy(F.year("claim_date")).orderBy(F.col("claim_date").desc(), "id", "claim_id")
w_claim_date_year_by_claim_dat_3 = Window.partitionBy(F.year("claim_date")).orderBy(F.col("claim_date").desc(), "id", "claim_id").rowsBetween(-3, -1)

cleaned_claims = (
    df_claims
    .filter((F.col("policy_id") >= 7) & (F.col("claimed_amount") > 19347.2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        (F.col("reported_at") + F.expr("INTERVAL 1 DAYS")).alias("reported_at_plus_1d"),
    )
)

screened = (
    cleaned_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_type").isin("delay", "flood", "theft")), "left_semi")
)

line_level = (
    screened.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_date").between(F.lit("2024-04-27").cast("date"), F.lit("2025-04-17").cast("date"))), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at_plus_1d",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

sequenced = (
    line_level
    .select(
        "*",
        F.sum("claimed_amount").over(w_claim_date_year_by_claim_dat).alias("claimed_amount_sum_7"),
        F.size(F.collect_set("claim_id").over(w_claim_date_year_policy_id)).alias("distinct_claim_id_in_claim_date_year_policy_id"),
        F.min("claimed_amount").over(w_claim_date_year_policy_id).alias("claim_date_year_policy_id_min_claimed_amount"),
        F.size(F.collect_set("claim_type").over(w_claim_date_year_policy_id)).alias("distinct_claim_type_in_claim_date_year_policy_id"),
        F.ntile(4).over(w_claim_date_year_by_claim_dat_2).alias("claim_date_quartile"),
        F.round(F.avg("claim_approved_amount").over(w_claim_date_year_by_claim_dat_3), 2).alias("claim_approved_amount_avg_3_prior"),
    )
)

result = (
    sequenced
    .cube("adjuster_id", "claim_id", "claim_date_quartile")
    .agg(
        F.max("claim_approved_amount_avg_3_prior").alias("max_claim_approved_amount_avg_3_prior"),
        F.count("*").alias("row_count"),
        F.countDistinct("claim_policy_id").alias("distinct_claim_policy_ids"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
        F.col("claim_date_quartile").asc_nulls_last(),
    )
)
