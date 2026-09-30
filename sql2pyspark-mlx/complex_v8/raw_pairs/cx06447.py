from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at = Window.orderBy(F.col("reported_at").desc(), "id")
w_policy_id_claim_type_by_appr = Window.partitionBy("policy_id", "claim_type").orderBy("approved_amount")
w_policy_status_adjuster_id_by = Window.partitionBy("policy_status", "adjuster_id").orderBy(F.col("adjuster_level").asc_nulls_last(), "id")
w_adjuster_level = Window.partitionBy("adjuster_level")
w_policy_id_claim_type_by_appr_2 = Window.partitionBy("policy_id", "claim_type").orderBy("approved_amount", "id")
w_policy_id_claim_type_by_appr_3 = Window.partitionBy("policy_id", "claim_type").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at_full = Window.orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_total_adjuster_level = Window.orderBy(F.col("total_adjuster_level_seq").desc(), "adjuster_team")
w_n_rows_adjuster_team = Window.partitionBy("n_rows", "adjuster_team")

filtered_claims = (
    df_claims
    .filter((F.to_date(F.col("reported_at")) < F.lit("2025-03-02").cast("date")) & (F.col("adjuster_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

with_parent = (
    filtered_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

metrics = (
    with_parent
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "claim_type_ref",
        "claim_type_or_unknown",
        "policy_start_date",
        "policy_status",
        "adjuster_level",
        "adjuster_team",
        F.round(F.cume_dist().over(w_all_by_reported_at), 3).alias("reported_at_cume_dist"),
        F.round(F.cume_dist().over(w_policy_id_claim_type_by_appr), 3).alias("approved_amount_cume_dist"),
        F.row_number().over(w_policy_status_adjuster_id_by).alias("adjuster_level_seq"),
        F.lead("claimed_amount").over(w_all_by_reported_at).alias("next_claimed_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_level), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_level"),
        F.sum("claimed_amount").over(w_adjuster_level).alias("adjuster_level_sum_claimed_amount"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_policy_id_claim_type_by_appr_2)).alias("approved_amount_change"),
        F.first("approved_amount", ignorenulls=True).over(w_policy_id_claim_type_by_appr_3).alias("first_known_approved_amount"),
        F.row_number().over(w_policy_id_claim_type_by_appr_2).alias("rn"),
        F.ntile(5).over(w_all_by_reported_at).alias("reported_at_quintile"),
        F.ntile(5).over(w_policy_status_adjuster_id_by).alias("adjuster_level_quintile"),
        F.round(F.percent_rank().over(w_policy_status_adjuster_id_by), 3).alias("adjuster_level_percent_rank"),
        F.last("claimed_amount").over(w_all_by_reported_at_full).alias("last_claimed_amount"),
    )
)

stats = (
    metrics
    .filter(F.col("claim_date").isNotNull())
    .groupBy("adjuster_team")
    .agg(
        F.round(F.sum("reported_at_quintile") / F.nullif(F.sum("adjuster_level_percent_rank"), F.lit(0)), 3).alias("reported_at_quintile_to_adjuster_level_percent_r"),
        F.sum("adjuster_level_seq").alias("total_adjuster_level_seq"),
        F.round(F.stddev("reported_at_quintile"), 2).alias("stddev_reported_at_quintile"),
        F.sum("reported_at_quintile").alias("total_reported_at_quintile"),
        F.sum("adjuster_level").alias("total_adjuster_level"),
        F.count("*").alias("n_rows"),
    )
)

ranked = (
    stats
    .select(
        "adjuster_team",
        "stddev_reported_at_quintile",
        "total_adjuster_level",
        "n_rows",
        F.lead("n_rows").over(w_all_by_total_adjuster_level).alias("next_n_rows"),
        F.max("stddev_reported_at_quintile").over(w_n_rows_adjuster_team).alias("n_rows_adjuster_team_max_stddev_reported_at_quin"),
    )
)

result = (
    ranked
    .select(
        "adjuster_team",
        "stddev_reported_at_quintile",
        "total_adjuster_level",
        "n_rows",
        "next_n_rows",
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_team")), " ", "_")).alias("adjuster_team_slug"),
        (F.floor(F.col("total_adjuster_level") / 5) * 5).alias("total_adjuster_level_bucket"),
    )
    .orderBy(
        F.col("total_adjuster_level").desc(),
        F.col("next_n_rows").asc_nulls_last(),
    )
)
