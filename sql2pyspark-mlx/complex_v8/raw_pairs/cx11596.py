from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_adjuster_level_b = Window.partitionBy("adjuster_id", "adjuster_level").orderBy("approved_amount", "id")
w_policy_id_reported_at_to_dat = Window.partitionBy("policy_id", F.to_date("reported_at"))
w_adjuster_id = Window.partitionBy("adjuster_id")
w_reported_at_to_date_by_claim = Window.partitionBy(F.to_date("reported_at")).orderBy(F.col("claimed_amount").desc(), "id")
w_adjuster_level_adjuster_team = Window.partitionBy("adjuster_level", "adjuster_team").orderBy(F.col("reported_at").desc(), "id")
w_adjuster_level = Window.partitionBy("adjuster_level")
w_adjuster_team_reported_at_mo = Window.partitionBy("adjuster_team", F.date_trunc("month", "reported_at")).orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_adjuster_level_b_2 = Window.partitionBy("adjuster_id", "adjuster_level").orderBy("approved_amount", "id").rowsBetween(-3, Window.currentRow)
w_adjuster_id_adjuster_level_b_3 = Window.partitionBy("adjuster_id", "adjuster_level").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_policy_id_in_adjust = Window.partitionBy("distinct_policy_id_in_adjuster_id")

scoped = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-04-25").cast("date")) & (F.col("claimed_amount") <= 41760.8))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

joined_adjuster = (
    scoped.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_type_clean",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

ranked = (
    joined_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claim_type_clean",
        "adjuster_name",
        "adjuster_team",
        F.lead("approved_amount").over(w_adjuster_id_adjuster_level_b).alias("next_approved_amount"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_policy_id_reported_at_to_dat), 2).alias("adjuster_level_vs_policy_id_reported_at_to_date_"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_reported_at_to_date_by_claim)).alias("adjuster_level_change"),
        F.lead("claimed_amount").over(w_adjuster_level_adjuster_team).alias("next_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id_reported_at_to_dat), 2).alias("claimed_amount_vs_policy_id_reported_at_to_date_"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_level), 2).alias("claimed_amount_vs_adjuster_level_avg"),
        F.first("adjuster_level").over(w_adjuster_team_reported_at_mo).alias("first_adjuster_level"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_adjuster_level), 2).alias("adjuster_level_vs_adjuster_level_avg"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_adjuster_level_b_2), 2).alias("claimed_amount_avg_4"),
        F.sum("approved_amount").over(w_adjuster_id_adjuster_level_b_3).alias("running_approved_amount"),
    )
)

labeled = (
    ranked
    .select(
        "*",
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
        F.concat(F.col("claim_type_clean"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_clean_ref"),
    )
)

sequenced = (
    labeled
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claim_type_clean",
        "adjuster_name",
        "adjuster_team",
        "adjuster_level_vs_policy_id_reported_at_to_date_",
        "distinct_policy_id_in_adjuster_id",
        "adjuster_level_change",
        "next_claimed_amount",
        "claimed_amount_vs_policy_id_reported_at_to_date_",
        "claimed_amount_vs_adjuster_level_avg",
        "adjuster_level_vs_adjuster_level_avg",
        "claimed_amount_avg_4",
        "running_approved_amount",
        "adjuster_name_len",
        "claim_type_clean_ref",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_distinct_policy_id_in_adjust), F.lit(0)), 4).alias("approved_amount_share_of_distinct_policy_id_in_a"),
        F.max("next_claimed_amount").over(w_distinct_policy_id_in_adjust).alias("distinct_policy_id_in_adjuster_id_max_next_claim"),
        F.count(F.lit(1)).over(w_distinct_policy_id_in_adjust).alias("rows_in_distinct_policy_id_in_adjuster_id"),
    )
)

result = (
    sequenced
    .select(
        "claim_type_clean_ref",
        "claimed_amount_vs_adjuster_level_avg",
        "running_approved_amount",
    )
    .distinct()
)
