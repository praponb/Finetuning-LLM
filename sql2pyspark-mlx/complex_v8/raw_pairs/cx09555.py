from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_to_date = Window.partitionBy(F.to_date("reported_at"))
w_adjuster_id_claim_type_by_cl = Window.partitionBy("adjuster_id", "claim_type").orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
w_all_by_greatest_approved_amo = Window.orderBy("greatest_approved_amount_claimed_amount", "id")
w_claim_type_reported_at_to_da = Window.partitionBy("claim_type", F.to_date("reported_at")).orderBy(F.col("claim_date").desc(), "id")
w_latest = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())
w_all_by_days_prev_claim_date = Window.orderBy("days_prev_claim_date_to_policy_start_date")
w_adjuster_id = Window.partitionBy("adjuster_id")

base_claims = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2024-12-02").cast("date")) & (F.col("claim_type").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.round(F.col("approved_amount") * 1.5, 2).alias("approved_amount_adj"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

sequenced = (
    base_claims
    .select(
        "*",
        F.round(F.col("days_since_reported_at") - F.avg("days_since_reported_at").over(w_reported_at_to_date), 2).alias("days_since_reported_at_vs_reported_at_to_date_av"),
        F.lag("claim_date").over(w_adjuster_id_claim_type_by_cl).alias("prev_claim_date"),
        F.round(F.percent_rank().over(w_all_by_greatest_approved_amo), 3).alias("greatest_approved_amount_claimed_amount_percent_"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_type_reported_at_to_da)).alias("days_since_prev_claim_date"),
        F.round(F.percent_rank().over(w_adjuster_id_claim_type_by_cl), 3).alias("claimed_amount_percent_rank"),
    )
)

enriched = (
    sequenced
    .filter(F.col("adjuster_id") <= 6)
    .select(
        "*",
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        F.date_format(F.col("prev_claim_date"), "MMM yyyy").alias("prev_claim_date_label"),
    )
)

with_details_adjuster = (
    enriched.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.approved_amount_adj",
        "s.days_since_reported_at",
        "s.greatest_approved_amount_claimed_amount",
        "s.days_since_reported_at_vs_reported_at_to_date_av",
        "s.prev_claim_date",
        "s.greatest_approved_amount_claimed_amount_percent_",
        "s.days_since_prev_claim_date",
        "s.claimed_amount_percent_rank",
        "s.approved_amount_per_claimed_amount",
        "s.prev_claim_date_label",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
    )
)

latest = (
    with_details_adjuster
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched_2 = (
    latest
    .select(
        "*",
        F.datediff(F.col("policy_start_date"), F.col("prev_claim_date")).alias("days_prev_claim_date_to_policy_start_date"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("policy_start_date")).alias("days_since_policy_start_date"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

ranked = (
    enriched_2
    .select(
        "id",
        "policy_id",
        "claim_type",
        "approved_amount",
        "approved_amount_adj",
        "days_since_reported_at",
        "greatest_approved_amount_claimed_amount",
        "prev_claim_date",
        "greatest_approved_amount_claimed_amount_percent_",
        "claimed_amount_percent_rank",
        "approved_amount_per_claimed_amount",
        "prev_claim_date_label",
        "adjuster_name",
        "policy_status",
        "claim_type_len",
        F.rank().over(w_all_by_days_prev_claim_date).alias("days_prev_claim_date_to_policy_start_date_rank"),
        F.round(F.avg("days_since_policy_start_date").over(w_adjuster_id), 2).alias("adjuster_id_avg_days_since_policy_start_date"),
        F.dense_rank().over(w_all_by_days_prev_claim_date).alias("days_prev_claim_date_to_policy_start_date_dense_"),
    )
)

result = (
    ranked
    .cube("claim_type_len", "days_prev_claim_date_to_policy_start_date_rank")
    .agg(
        F.round(F.sum("claimed_amount_percent_rank") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("claimed_amount_percent_rank_to_approved_amount_r"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
    )
)
