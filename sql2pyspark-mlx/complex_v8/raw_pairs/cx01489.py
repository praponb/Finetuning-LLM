from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_plus_30d_year_pol = Window.partitionBy("claim_date_plus_30d_year", "policy_id").orderBy("policy_premium", "id")
w_policy_id = Window.partitionBy("policy_id")
w_adjuster_level_by_adjuster_l = Window.partitionBy("adjuster_level").orderBy(F.col("adjuster_level").desc(), "id")

cleaned = (
    df_claims
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
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.date_add(F.col("claim_date"), 30).alias("claim_date_plus_30d"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_filled",
        "s.claim_date_plus_30d",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        F.col("p.premium").alias("policy_premium"),
    )
)

derived = (
    joined
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "claimed_amount_filled",
        "claim_date_plus_30d",
        "adjuster_level",
        "policy_premium",
        F.year(F.col("claim_date_plus_30d")).alias("claim_date_plus_30d_year"),
    )
)

with_windows = (
    derived
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "claimed_amount_filled",
        "adjuster_level",
        "claim_date_plus_30d_year",
        F.lag("adjuster_level").over(w_claim_date_plus_30d_year_pol).alias("prev_adjuster_level"),
        F.size(F.collect_set("claim_type").over(w_policy_id)).alias("distinct_claim_type_in_policy_id"),
        F.sum("claimed_amount_filled").over(w_policy_id).alias("policy_id_sum_claimed_amount_filled"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_adjuster_level_by_adjuster_l)).alias("approved_amount_change"),
        F.dense_rank().over(w_adjuster_level_by_adjuster_l).alias("adjuster_level_dense_rank"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["prev_adjuster_level", "distinct_claim_type_in_policy_id"], ["prev_adjuster_level"], ["distinct_claim_type_in_policy_id"], []],
        "prev_adjuster_level", "distinct_claim_type_in_policy_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.percentile_approx("policy_id_sum_claimed_amount_filled", 0.5).alias("median_policy_id_sum_claimed_amount_filled"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("prev_adjuster_level").asc_nulls_last(),
        F.col("distinct_claim_type_in_policy_id").asc_nulls_last(),
    )
)
