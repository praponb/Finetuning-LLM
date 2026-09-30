from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_adjuster_team = Window.partitionBy("adjuster_id", "adjuster_team")
w_policy_status = Window.partitionBy("policy_status")
w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy("claimed_amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_adjuster_level = Window.partitionBy("policy_id").orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_adjuster_level_2 = Window.partitionBy("policy_id").orderBy("adjuster_level")
w_claim_date_month_by_claimed = Window.partitionBy(F.month("claim_date")).orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_claim_date_month_by_claimed_2 = Window.partitionBy(F.month("claim_date")).orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_claim_date_month_by_claimed_3 = Window.partitionBy(F.month("claim_date")).orderBy(F.col("claimed_amount").asc_nulls_last())

scoped = (
    df_claims
    .filter(F.col("claim_type") == "flood")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.date_trunc("hour", F.col("reported_at")).alias("reported_at_hour"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

with_details = (
    scoped.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
    )
)

with_trends = (
    with_details
    .select(
        "*",
        F.size(F.collect_set("policy_id").over(w_adjuster_id_adjuster_team)).alias("distinct_policy_id_in_adjuster_id_adjuster_team"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_policy_status), F.lit(0)), 4).alias("adjuster_level_share_of_policy_status"),
        F.first("reported_at_hour").over(w_claim_type_by_claimed_amount).alias("first_reported_at_hour"),
        F.size(F.collect_set("policy_status").over(w_adjuster_id_adjuster_team)).alias("distinct_policy_status_in_adjuster_id_adjuster_t"),
        F.last("reported_at_hour", ignorenulls=True).over(w_policy_id_by_adjuster_level).alias("last_known_reported_at_hour"),
        F.round(F.cume_dist().over(w_policy_id_by_adjuster_level_2), 3).alias("adjuster_level_cume_dist"),
        F.round(F.avg("claimed_amount_filled").over(w_claim_date_month_by_claimed), 2).alias("claimed_amount_filled_avg_4"),
        F.round(F.avg("claimed_amount").over(w_claim_date_month_by_claimed_2), 2).alias("claimed_amount_avg_2"),
        F.round((F.unix_timestamp("reported_at_hour") - F.unix_timestamp(F.lag("reported_at_hour").over(w_all_by_claimed_amount))) / 60, 1).alias("minutes_since_prev_reported_at_hour"),
        F.round(F.cume_dist().over(w_claim_date_month_by_claimed_3), 3).alias("claimed_amount_cume_dist"),
    )
)

enriched = (
    with_trends
    .filter(F.col("adjuster_level_cume_dist") >= 0.4)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at_hour",
        "claimed_amount_filled",
        "adjuster_level",
        "distinct_policy_status_in_adjuster_id_adjuster_t",
        "last_known_reported_at_hour",
        "adjuster_level_cume_dist",
        "claimed_amount_filled_avg_4",
        "claimed_amount_avg_2",
        "minutes_since_prev_reported_at_hour",
        "claimed_amount_cume_dist",
        (F.col("claimed_amount_cume_dist") + F.col("adjuster_level")).alias("claimed_amount_cume_dist_plus_adjuster_level"),
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.datediff(F.col("policy_end_date"), F.col("claim_date")).alias("days_claim_date_to_policy_end_date"),
    )
)

per_group = (
    enriched
    .filter(F.col("reported_at_hour") >= F.lit("2025-01-21 09:00:00").cast("timestamp"))
    .groupBy("adjuster_level")
    .agg(
        F.sum("claim_date_quarter").alias("total_claim_date_quarter"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    per_group
    .select(
        "adjuster_level",
        "n_rows",
        F.abs(F.col("total_claim_date_quarter") - 16).alias("total_claim_date_quarter_distance"),
        F.round(F.col("total_claim_date_quarter") / F.nullif(F.col("adjuster_level"), F.lit(0)), 3).alias("total_claim_date_quarter_per_adjuster_level"),
    )
    .orderBy("n_rows", F.col("total_claim_date_quarter_per_adjuster_level").asc())
)
