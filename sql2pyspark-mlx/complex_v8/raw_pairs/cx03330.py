from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount_prior = Window.orderBy(F.col("approved_amount").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_all_by_claimed_amount_moving = Window.orderBy("claimed_amount", "id").rowsBetween(-3, Window.currentRow)
w_claim_type = Window.partitionBy("claim_type")
w_all_by_approved_amount_runni = Window.orderBy(F.col("approved_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_all_by_rows_in_claim_type_fu = Window.orderBy("rows_in_claim_type", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_claimed_amount_t = Window.partitionBy("adjuster_id", "claimed_amount_tier")
w_all_by_rows_in_claim_type = Window.orderBy("rows_in_claim_type", "id")

prepared = (
    df_claims
    .filter(F.year(F.col("claim_date")) == 2024)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.when(F.col("claimed_amount") >= 28900.9, "A").when(F.col("claimed_amount") >= 18364.1, "B").otherwise("C").alias("claimed_amount_tier"),
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.sum("approved_amount").over(w_all_by_approved_amount_prior).alias("approved_amount_sum_6_prior"),
        F.max("claimed_amount").over(w_all_by_claimed_amount_moving).alias("claimed_amount_max_4"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_claim_type"),
        F.last("claimed_amount", ignorenulls=True).over(w_all_by_approved_amount_runni).alias("last_known_claimed_amount"),
        F.round(F.percent_rank().over(w_claim_type_by_approved_amoun), 3).alias("approved_amount_percent_rank"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "claimed_amount_tier",
        "claimed_amount_minus_approved_amount",
        "approved_amount_sum_6_prior",
        "claimed_amount_max_4",
        "approved_amount_share_of_claim_type",
        "last_known_claimed_amount",
        "approved_amount_percent_rank",
        "rows_in_claim_type",
        F.nth_value("last_known_claimed_amount", 2).over(w_all_by_rows_in_claim_type_fu).alias("second_last_known_claimed_amount"),
        F.max("approved_amount_percent_rank").over(w_adjuster_id_claimed_amount_t).alias("adjuster_id_claimed_amount_tier_max_approved_amo"),
        F.round(F.percent_rank().over(w_all_by_rows_in_claim_type), 3).alias("rows_in_claim_type_percent_rank"),
        F.lag("last_known_claimed_amount").over(w_all_by_rows_in_claim_type).alias("prev_last_known_claimed_amount"),
        F.size(F.collect_set("claim_type").over(w_adjuster_id_claimed_amount_t)).alias("distinct_claim_type_in_adjuster_id_claimed_amoun"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id_claimed_amount_t)).alias("distinct_policy_id_in_adjuster_id_claimed_amount"),
    )
)

qualified = (
    metrics.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") < 32796.4), "left_anti")
)

exploded_detail = (
    qualified.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount_tier",
        "s.claimed_amount_minus_approved_amount",
        "s.claimed_amount_max_4",
        "s.approved_amount_share_of_claim_type",
        "s.last_known_claimed_amount",
        "s.approved_amount_percent_rank",
        "s.rows_in_claim_type_percent_rank",
        "s.distinct_claim_type_in_adjuster_id_claimed_amoun",
        "s.distinct_policy_id_in_adjuster_id_claimed_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

with_activity = (
    exploded_detail.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status").isin("OPEN", "APPROVED", "REJECTED")), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "claim_id",
        "distinct_policy_id_in_adjuster_id_claimed_amount",
        "distinct_claim_type_in_adjuster_id_claimed_amoun",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["distinct_policy_id_in_adjuster_id_claimed_amount", "distinct_claim_type_in_adjuster_id_claimed_amoun"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "claim_id", "metric")
)
