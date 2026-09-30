from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_len_by_claim_date = Window.partitionBy("status_len").orderBy(F.col("claim_date").desc(), "id")
w_adjuster_level_adjuster_team = Window.partitionBy("adjuster_level", "adjuster_team")
w_all_by_adjuster_level_runnin = Window.orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-03-31").cast("date")) | (F.col("claim_type") == "fire"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

with_details = (
    scoped.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.claim_type_clean",
        "s.claimed_amount_filled",
        "s.status_len",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "claim_type_clean",
        "claimed_amount_filled",
        "status_len",
        "adjuster_level",
        "adjuster_name",
        "adjuster_team",
        F.lead("status_len", 2, 0).over(w_status_len_by_claim_date).alias("next2_status_len"),
        F.ntile(5).over(w_status_len_by_claim_date).alias("claim_date_quintile"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "claimed_amount_filled",
        "status_len",
        "adjuster_name",
        "adjuster_team",
        F.round(F.col("claim_date_quintile") / F.nullif(F.sum("claim_date_quintile").over(w_adjuster_level_adjuster_team), F.lit(0)), 4).alias("claim_date_quintile_share_of_adjuster_level_adju"),
        F.sum("adjuster_level").over(w_all_by_adjuster_level_runnin).alias("running_adjuster_level"),
    )
)

result = (
    with_trends_2
    .groupingSets(
        [["claim_type", "status", "status_len"], ["claim_type"], ["status"], []],
        "claim_type", "status", "status_len",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("claim_date_quintile_share_of_adjuster_level_adju").alias("total_claim_date_quintile_share_of_adjuster_leve"),
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("status_len").asc_nulls_last(),
    )
)
