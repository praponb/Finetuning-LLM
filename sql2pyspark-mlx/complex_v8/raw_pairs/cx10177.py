from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_claim_date_dayofweek = Window.partitionBy("status", "claim_date_dayofweek").orderBy("policy_end_date", "id")
w_adjuster_team_status_by_poli = Window.partitionBy("adjuster_team", "status").orderBy("policy_end_date", "id")
w_status_adjuster_team = Window.partitionBy("status", "adjuster_team")
w_all_by_reported_at_full = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type = Window.partitionBy("claim_type")
w_adjuster_team_status_by_poli_2 = Window.partitionBy("adjuster_team", "status").orderBy("policy_end_date")
w_adjuster_level_claim_date_mo = Window.partitionBy("adjuster_level", F.date_trunc("month", "claim_date"))
w_all_by_reported_at_prior_3 = Window.orderBy("reported_at", "id").rowsBetween(-3, -1)
w_adjuster_team_status_by_poli_3 = Window.partitionBy("adjuster_team", "status").orderBy("policy_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_adjuster_team_in_ad = Window.partitionBy("distinct_adjuster_team_in_adjuster_level_claim_d")
w_policy_end_date_decile_by_se = Window.partitionBy("policy_end_date_decile").orderBy(F.col("second_policy_end_date").desc(), "id")

prepared = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("claim_date").isNotNull()))
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
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
)

joined_adjuster = (
    prepared.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

metrics = (
    joined_adjuster
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "adjuster_level",
        "adjuster_name",
        "adjuster_team",
        "policy_end_date",
        F.min("adjuster_level").over(w_claim_type_by_claimed_amount).alias("min_adjuster_level_so_far"),
        F.ntile(10).over(w_status_claim_date_dayofweek).alias("policy_end_date_decile"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_adjuster_team_status_by_poli)).alias("claimed_amount_change"),
        F.sum("adjuster_level").over(w_status_adjuster_team).alias("status_adjuster_team_sum_adjuster_level"),
        F.nth_value("policy_end_date", 2).over(w_all_by_reported_at_full).alias("second_policy_end_date"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type), 2).alias("approved_amount_vs_claim_type_avg"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status_adjuster_team), 2).alias("approved_amount_vs_status_adjuster_team_avg"),
        F.round(F.percent_rank().over(w_adjuster_team_status_by_poli_2), 3).alias("policy_end_date_percent_rank"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_level_claim_date_mo), 2).alias("claimed_amount_vs_adjuster_level_claim_date_mont"),
        F.round(F.avg("approved_amount").over(w_all_by_reported_at_prior_3), 2).alias("approved_amount_avg_3_prior"),
        F.max("claimed_amount").over(w_status_adjuster_team).alias("status_adjuster_team_max_claimed_amount"),
        F.round(F.col("claim_date_dayofweek") - F.avg("claim_date_dayofweek").over(w_adjuster_level_claim_date_mo), 2).alias("claim_date_dayofweek_vs_adjuster_level_claim_dat"),
        F.nth_value("claim_date_dayofweek", 2).over(w_adjuster_team_status_by_poli_3).alias("second_claim_date_dayofweek"),
        F.size(F.collect_set("adjuster_team").over(w_adjuster_level_claim_date_mo)).alias("distinct_adjuster_team_in_adjuster_level_claim_d"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_claim_type), 2).alias("adjuster_level_vs_claim_type_avg"),
    )
)

tagged = (
    metrics
    .filter(F.col("min_adjuster_level_so_far").between(6, 43))
    .withColumn("greatest_claimed_amount_change_adjuster_level", F.greatest(F.col("claimed_amount_change"), F.col("adjuster_level")))
)

sequenced = (
    tagged
    .select(
        "*",
        F.round(F.col("policy_end_date_percent_rank") - F.avg("policy_end_date_percent_rank").over(w_distinct_adjuster_team_in_ad), 2).alias("policy_end_date_percent_rank_vs_distinct_adjuste"),
        F.sum("adjuster_level").over(w_distinct_adjuster_team_in_ad).alias("distinct_adjuster_team_in_adjuster_level_claim_d_2"),
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_policy_end_date_decile_by_se)).alias("adjuster_level_change"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_distinct_adjuster_team_in_ad), F.lit(0)), 4).alias("adjuster_level_share_of_distinct_adjuster_team_i"),
        F.size(F.collect_set("claim_type").over(w_distinct_adjuster_team_in_ad)).alias("distinct_claim_type_in_distinct_adjuster_team_in"),
        F.max("approved_amount_avg_3_prior").over(w_distinct_adjuster_team_in_ad).alias("distinct_adjuster_team_in_adjuster_level_claim_d_3"),
    )
)

rollup_base = (
    sequenced
    .filter(F.col("distinct_adjuster_team_in_adjuster_level_claim_d_3") > 125000.0)
    .groupBy("policy_end_date_decile", "adjuster_id")
    .agg(
        F.round(F.stddev("approved_amount_vs_status_adjuster_team_avg"), 2).alias("stddev_approved_amount_vs_status_adjuster_team_a"),
        F.round(F.stddev("approved_amount"), 2).alias("stddev_approved_amount"),
        F.round(F.sum("distinct_adjuster_team_in_adjuster_level_claim_d_2") / F.nullif(F.sum("policy_end_date_decile"), F.lit(0)), 3).alias("distinct_adjuster_team_in_adjuster_level_claim_d_4"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
    )
)

result = (
    rollup_base
    .filter(F.col("policy_end_date_decile") >= 5)
    .select(
        "policy_end_date_decile",
        "stddev_approved_amount",
        "distinct_adjuster_ids",
        F.round(F.col("distinct_adjuster_ids") / F.nullif(F.col("stddev_approved_amount"), F.lit(0)), 3).alias("distinct_adjuster_ids_per_stddev_approved_amount"),
    )
    .orderBy(
        "stddev_approved_amount",
        F.col("distinct_adjuster_ids_per_stddev_approved_amount").desc(),
    )
)
