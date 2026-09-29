from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_adjuster_level_adjuster_id_b = Window.partitionBy("adjuster_level", "adjuster_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_adjuster_level_by = Window.partitionBy("policy_id", "adjuster_level").orderBy("adjuster_level")
w_claimed_amount_tier_policy_i = Window.partitionBy("claimed_amount_tier", "policy_id").orderBy("adjuster_level", "id")
w_claimed_amount_tier_policy_i_2 = Window.partitionBy("claimed_amount_tier", "policy_id").orderBy(F.col("adjuster_level").desc(), "id")
w_adjuster_level_adjuster_id_b_2 = Window.partitionBy("adjuster_level", "adjuster_id").orderBy("approved_amount")
w_all_by_claim_date_prior_2 = Window.orderBy("claim_date", "id").rowsBetween(-2, -1)
w_adjuster_level = Window.partitionBy("adjuster_level")
w_reported_at_month_adjuster_i = Window.partitionBy(F.month("reported_at"), "adjuster_id")
w_claim_type_by_minutes_since = Window.partitionBy("claim_type").orderBy("minutes_since_prev_reported_at", "id")
w_claim_type_by_minutes_since_2 = Window.partitionBy("claim_type").orderBy("minutes_since_prev_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_claims = (
    df_claims
    .filter((F.col("status").isNotNull()) | (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.when(F.col("claimed_amount") >= 29336.0, "large").when(F.col("claimed_amount") >= 20436.5, "medium").otherwise("small").alias("claimed_amount_tier"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
    )
)

combined_details_adjuster = (
    filtered_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.level") <= 3)
    .select("s.*", F.col("a.level").alias("adjuster_level"))
)

sequenced = (
    combined_details_adjuster
    .select(
        "*",
        F.round(F.avg("approved_amount").over(w_adjuster_id_policy_id), 2).alias("adjuster_id_policy_id_avg_approved_amount"),
        F.sum("adjuster_level").over(w_adjuster_level_adjuster_id_b).alias("running_adjuster_level"),
        F.rank().over(w_policy_id_adjuster_level_by).alias("adjuster_level_rank"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_claimed_amount_tier_policy_i))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.row_number().over(w_claimed_amount_tier_policy_i_2).alias("adjuster_level_seq"),
        F.dense_rank().over(w_adjuster_level_adjuster_id_b_2).alias("approved_amount_dense_rank"),
        F.count(F.lit(1)).over(w_adjuster_id_policy_id).alias("rows_in_adjuster_id_policy_id"),
        F.max("approved_amount").over(w_all_by_claim_date_prior_2).alias("approved_amount_max_2_prior"),
        F.min("adjuster_level").over(w_adjuster_level).alias("adjuster_level_min_adjuster_level"),
        F.sum("approved_amount").over(w_adjuster_id_policy_id).alias("adjuster_id_policy_id_sum_approved_amount"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "reported_at",
        "claimed_amount_tier",
        "claim_type_ref",
        "adjuster_level",
        "adjuster_id_policy_id_avg_approved_amount",
        "running_adjuster_level",
        "adjuster_level_rank",
        "adjuster_level_seq",
        "approved_amount_max_2_prior",
        "adjuster_level_min_adjuster_level",
        "adjuster_id_policy_id_sum_approved_amount",
        F.min("running_adjuster_level").over(w_reported_at_month_adjuster_i).alias("reported_at_month_adjuster_id_min_running_adjust"),
        F.ntile(10).over(w_claim_type_by_minutes_since).alias("minutes_since_prev_reported_at_decile"),
        F.sum("adjuster_level_seq").over(w_reported_at_month_adjuster_i).alias("reported_at_month_adjuster_id_sum_adjuster_level"),
        F.nth_value("claim_date", 2).over(w_claim_type_by_minutes_since_2).alias("second_claim_date"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_reported_at_month_adjuster_i), 2).alias("approved_amount_vs_reported_at_month_adjuster_id"),
        F.dense_rank().over(w_claim_type_by_minutes_since).alias("minutes_since_prev_reported_at_dense_rank"),
        F.round(F.col("approved_amount_max_2_prior") / F.nullif(F.sum("approved_amount_max_2_prior").over(w_reported_at_month_adjuster_i), F.lit(0)), 4).alias("approved_amount_max_2_prior_share_of_reported_at"),
    )
)

result = (
    metrics
    .select(
        "id",
        "adjuster_id",
        "reported_at",
        "claimed_amount_tier",
        "adjuster_level",
        "adjuster_id_policy_id_avg_approved_amount",
        "adjuster_level_rank",
        "adjuster_level_seq",
        "approved_amount_max_2_prior",
        "adjuster_level_min_adjuster_level",
        "adjuster_id_policy_id_sum_approved_amount",
        "reported_at_month_adjuster_id_min_running_adjust",
        "minutes_since_prev_reported_at_decile",
        "reported_at_month_adjuster_id_sum_adjuster_level",
        "second_claim_date",
        "minutes_since_prev_reported_at_dense_rank",
        F.coalesce(F.col("running_adjuster_level"), F.lit(0)).alias("running_adjuster_level_filled"),
        F.when(F.col("adjuster_level_seq") >= 8, "HIGH").when(F.col("adjuster_level_seq") >= 4, "MEDIUM").otherwise("LOW").alias("adjuster_level_seq_tier"),
    )
    .orderBy(
        F.col("adjuster_id_policy_id_sum_approved_amount").asc(),
        F.col("adjuster_id").desc(),
        "id",
    )
    .limit(10)
)
