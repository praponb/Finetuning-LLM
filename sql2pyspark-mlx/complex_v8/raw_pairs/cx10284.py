from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_adjuster_level_mov = Window.partitionBy("status").orderBy(F.col("adjuster_level").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_adjuster_level_by_reported_a = Window.partitionBy("adjuster_level").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_to_date_status_b = Window.partitionBy(F.to_date("reported_at"), "status").orderBy(F.col("adjuster_level").desc(), "id")
w_claim_type_status = Window.partitionBy("claim_type", "status")
w_status_by_adjuster_level = Window.partitionBy("status").orderBy(F.col("adjuster_level").asc_nulls_last(), "id")
w_adjuster_level_by_reported_a_2 = Window.partitionBy("adjuster_level").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level_by_reported_a_3 = Window.partitionBy("adjuster_level").orderBy(F.col("reported_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_status_by_adjuster_level_run = Window.partitionBy("status").orderBy(F.col("adjuster_level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_adjuster_level_ch = Window.partitionBy("claim_type", "adjuster_level_change")
w_claim_type_adjuster_id_by_di = Window.partitionBy("claim_type", "adjuster_id").orderBy(F.col("distinct_policy_id_in_claim_type_status").desc(), "id").rowsBetween(-1, -1)
w_distinct_policy_id_in_claim = Window.partitionBy("distinct_policy_id_in_claim_type_status").orderBy("last_known_adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_adjuster_level_qui = Window.partitionBy("policy_id", "adjuster_level_quintile").orderBy(F.col("adjuster_level_cume_dist").desc(), "id")

filtered = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_details = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        "s.reported_at",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.premium").alias("policy_premium"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.round(F.avg("policy_premium").over(w_status_by_adjuster_level_mov), 2).alias("policy_premium_avg_7"),
        F.first("adjuster_level", ignorenulls=True).over(w_adjuster_level_by_reported_a).alias("first_known_adjuster_level"),
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_reported_at_to_date_status_b)).alias("adjuster_level_change"),
        F.min("policy_premium").over(w_claim_type_status).alias("claim_type_status_min_policy_premium"),
        F.size(F.collect_set("policy_id").over(w_claim_type_status)).alias("distinct_policy_id_in_claim_type_status"),
        F.sum("policy_premium").over(w_claim_type_status).alias("claim_type_status_sum_policy_premium"),
        F.ntile(5).over(w_status_by_adjuster_level).alias("adjuster_level_quintile"),
        F.last("adjuster_level").over(w_adjuster_level_by_reported_a_2).alias("last_adjuster_level"),
        F.max("adjuster_level").over(w_adjuster_level_by_reported_a_3).alias("adjuster_level_max_3"),
        F.round(F.cume_dist().over(w_status_by_adjuster_level), 3).alias("adjuster_level_cume_dist"),
        F.lead("reported_at", 2).over(w_reported_at_to_date_status_b).alias("next2_reported_at"),
        F.count(F.lit(1)).over(w_claim_type_status).alias("rows_in_claim_type_status"),
        F.last("adjuster_level", ignorenulls=True).over(w_status_by_adjuster_level_run).alias("last_known_adjuster_level"),
        F.lead("policy_premium").over(w_reported_at_to_date_status_b).alias("next_policy_premium"),
    )
)

enriched = (
    with_windows
    .filter(F.col("policy_premium_avg_7") < 4520.0)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_type_adjuster_level_ch).alias("rows_in_claim_type_adjuster_level_change"),
        F.round(F.avg("policy_premium").over(w_claim_type_adjuster_id_by_di), 2).alias("policy_premium_avg_1_prior"),
        F.first("first_known_adjuster_level", ignorenulls=True).over(w_distinct_policy_id_in_claim).alias("first_known_first_known_adjuster_level"),
        (F.col("next_policy_premium") - F.lag("next_policy_premium").over(w_policy_id_adjuster_level_qui)).alias("next_policy_premium_change"),
    )
)

result = (
    with_trends
    .filter(F.col("adjuster_id") >= 3)
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "adjuster_level",
        "policy_premium",
        "policy_premium_avg_7",
        "first_known_adjuster_level",
        "claim_type_status_min_policy_premium",
        "distinct_policy_id_in_claim_type_status",
        "adjuster_level_quintile",
        "adjuster_level_max_3",
        "adjuster_level_cume_dist",
        "next2_reported_at",
        "last_known_adjuster_level",
        "next_policy_premium",
        "status_slug",
        "policy_premium_avg_1_prior",
        "first_known_first_known_adjuster_level",
        "next_policy_premium_change",
        F.coalesce(F.col("policy_premium"), F.lit(0.0)).alias("policy_premium_filled"),
    )
    .orderBy("adjuster_level_quintile")
)
