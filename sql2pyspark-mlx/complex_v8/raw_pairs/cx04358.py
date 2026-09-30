from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_status_by_reported_at = Window.partitionBy("policy_status").orderBy(F.col("reported_at").desc(), "id")
w_adjuster_id_policy_holder_id = Window.partitionBy("adjuster_id", "policy_holder_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_status_reported_at_to = Window.partitionBy("policy_status", F.to_date("reported_at")).orderBy("reported_at", "id").rowsBetween(-2, -1)
w_policy_status_by_reported_at_2 = Window.partitionBy("policy_status").orderBy(F.col("reported_at").desc())
w_adjuster_id_policy_holder_id_2 = Window.partitionBy("adjuster_id", "policy_holder_id").orderBy("reported_at", "id")
w_policy_start_date_year_adjus = Window.partitionBy(F.year("policy_start_date"), "adjuster_id")
w_policy_status_reported_at_to_2 = Window.partitionBy("policy_status", F.to_date("reported_at")).orderBy("reported_at", "id")
w_policy_status_by_reported_at_3 = Window.partitionBy("policy_status").orderBy(F.col("reported_at").desc(), "id").rowsBetween(-1, -1)
w_rn_by_prev_claim_date = Window.partitionBy("rn").orderBy("prev_claim_date", "id")
w_all_by_first_known_claim_dat = Window.orderBy(F.col("first_known_claim_date").desc_nulls_first(), "id")
w_reported_at_quintile = Window.partitionBy("reported_at_quintile")
w_rn_by_prev_claim_date_ties = Window.partitionBy("rn").orderBy("prev_claim_date")
w_all_by_first_known_claim_dat_2 = Window.orderBy(F.col("first_known_claim_date").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)

scoped_claims = df_claims

joined_policy = (
    scoped_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.reported_at",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

with_windows = (
    joined_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "policy_start_date",
        F.rank().over(w_policy_status_by_reported_at).alias("reported_at_rank"),
        F.first("claim_date", ignorenulls=True).over(w_adjuster_id_policy_holder_id).alias("first_known_claim_date"),
        F.round(F.avg("claimed_amount").over(w_policy_status_reported_at_to), 2).alias("claimed_amount_avg_2_prior"),
        F.round(F.percent_rank().over(w_policy_status_by_reported_at_2), 3).alias("reported_at_percent_rank"),
        F.ntile(4).over(w_adjuster_id_policy_holder_id_2).alias("reported_at_quartile"),
        F.count(F.lit(1)).over(w_policy_start_date_year_adjus).alias("rows_in_policy_start_date_year_adjuster_id"),
        F.lag("claim_date").over(w_adjuster_id_policy_holder_id_2).alias("prev_claim_date"),
        F.rank().over(w_adjuster_id_policy_holder_id_2).alias("reported_at_rank_2"),
        F.row_number().over(w_policy_status_reported_at_to_2).alias("rn"),
        F.ntile(5).over(w_adjuster_id_policy_holder_id_2).alias("reported_at_quintile"),
        F.lag("reported_at").over(w_policy_status_by_reported_at).alias("prev_reported_at"),
        F.max("claimed_amount").over(w_policy_status_by_reported_at_3).alias("claimed_amount_max_1_prior"),
        F.round(F.percent_rank().over(w_policy_status_by_reported_at), 3).alias("reported_at_percent_rank_2"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "policy_start_date",
        "reported_at_rank",
        "first_known_claim_date",
        "claimed_amount_avg_2_prior",
        "reported_at_quartile",
        "rows_in_policy_start_date_year_adjuster_id",
        "prev_claim_date",
        "rn",
        "prev_reported_at",
        "claimed_amount_max_1_prior",
        "reported_at_percent_rank_2",
        F.ntile(4).over(w_rn_by_prev_claim_date).alias("prev_claim_date_quartile"),
        (F.col("reported_at_percent_rank_2") - F.lag("reported_at_percent_rank_2").over(w_all_by_first_known_claim_dat)).alias("reported_at_percent_rank_2_change"),
        F.round(F.col("reported_at_rank_2") - F.avg("reported_at_rank_2").over(w_reported_at_quintile), 2).alias("reported_at_rank_2_vs_reported_at_quintile_avg"),
        F.sum("reported_at_rank_2").over(w_reported_at_quintile).alias("reported_at_quintile_sum_reported_at_rank_2"),
        F.rank().over(w_rn_by_prev_claim_date_ties).alias("prev_claim_date_rank"),
        F.round(F.avg("reported_at_rank").over(w_all_by_first_known_claim_dat_2), 2).alias("reported_at_rank_avg_2"),
    )
)

summary = (
    with_windows_2
    .groupBy(
        F.year("prev_claim_date").alias("prev_claim_date_year"),
        "reported_at_rank",
    )
    .agg(
        F.countDistinct("claim_type").alias("distinct_claim_types"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("claim_type") == "flood", 1).otherwise(0)).alias("n_claim_type_flood"),
        F.sum(F.when(F.col("claim_type") != "flood", F.col("prev_claim_date_quartile"))).alias("prev_claim_date_quartile_claim_type_flood"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("claim_type") == "fire", 1)).alias("n_claim_type_fire"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    summary
    .select(
        "prev_claim_date_year",
        "reported_at_rank",
        "n_claim_type_flood",
        "prev_claim_date_quartile_claim_type_flood",
        "distinct_ids",
        "n_claim_type_fire",
    )
    .orderBy("n_claim_type_fire")
)
