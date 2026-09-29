from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_or_unknown = Window.partitionBy("status_or_unknown")
w_policy_id_status_or_unknown = Window.partitionBy("policy_id", "status_or_unknown").orderBy("claim_date", "id").rowsBetween(-1, -1)
w_status_or_unknown_adjuster_t = Window.partitionBy("status_or_unknown", "adjuster_team")
w_policy_id_status_or_unknown_2 = Window.partitionBy("policy_id", "status_or_unknown").orderBy("claim_date", "id")
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").asc_nulls_last())
w_adjuster_id_by_claimed_amoun_2 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_claimed_amoun_3 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").asc_nulls_last(), "id")

filtered_claims = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

with_parent = (
    filtered_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select("s.*", F.col("a.team").alias("adjuster_team"))
)

with_windows = (
    with_parent
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "status_or_unknown",
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status_or_unknown), F.lit(0)), 4).alias("claimed_amount_share_of_status_or_unknown"),
        F.round(F.avg("claimed_amount").over(w_policy_id_status_or_unknown), 2).alias("claimed_amount_avg_1_prior"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status_or_unknown), 2).alias("claimed_amount_vs_status_or_unknown_avg"),
        F.min("claimed_amount").over(w_status_or_unknown_adjuster_t).alias("status_or_unknown_adjuster_team_min_claimed_amou"),
        F.lead("claimed_amount").over(w_policy_id_status_or_unknown_2).alias("next_claimed_amount"),
        F.round(F.cume_dist().over(w_adjuster_id_by_claimed_amoun), 3).alias("claimed_amount_cume_dist"),
        F.size(F.collect_set("policy_id").over(w_status_or_unknown_adjuster_t)).alias("distinct_policy_id_in_status_or_unknown_adjuster"),
        F.first("claimed_amount").over(w_adjuster_id_by_claimed_amoun_2).alias("first_claimed_amount"),
        F.ntile(3).over(w_adjuster_id_by_claimed_amoun_3).alias("claimed_amount_tercile"),
        F.lag("claim_date", 2).over(w_adjuster_id_by_claimed_amoun_3).alias("prev2_claim_date"),
        F.count(F.lit(1)).over(w_status_or_unknown).alias("rows_in_status_or_unknown"),
        F.row_number().over(w_policy_id_status_or_unknown_2).alias("claim_date_seq"),
        F.size(F.collect_set("claim_type").over(w_status_or_unknown)).alias("distinct_claim_type_in_status_or_unknown"),
    )
)

tagged = with_windows.select("*", F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"))

result = (
    tagged
    .select(
        "id",
        "policy_id",
        "claim_type",
        "status_or_unknown",
        "claimed_amount_share_of_status_or_unknown",
        "status_or_unknown_adjuster_team_min_claimed_amou",
        "next_claimed_amount",
        "claimed_amount_cume_dist",
        "distinct_policy_id_in_status_or_unknown_adjuster",
        "first_claimed_amount",
        "claimed_amount_tercile",
        "prev2_claim_date",
        "rows_in_status_or_unknown",
        "claim_date_seq",
        "distinct_claim_type_in_status_or_unknown",
        "claim_type_len",
    )
    .orderBy(F.col("first_claimed_amount").desc())
)
