from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_plus_7d = Window.orderBy("reported_at_plus_7d", "id")
w_policy_id = Window.partitionBy("policy_id")
w_claim_type_by_reported_at_pl = Window.partitionBy("claim_type").orderBy("reported_at_plus_7d_decile")
w_policy_id_claim_type = Window.partitionBy("policy_id", "claim_type")
w_claim_type_by_reported_at_pl_2 = Window.partitionBy("claim_type").orderBy("reported_at_plus_7d_decile", "id").rowsBetween(-1, Window.currentRow)
w_reported_at_plus_7d_decile_p = Window.partitionBy("reported_at_plus_7d_decile", "policy_id")
w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy("claimed_amount", "id")

recent = (
    df_claims
    .filter((F.col("reported_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp")) & (F.col("claim_date") >= F.lit("2024-07-20").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
        (F.col("reported_at") + F.expr("INTERVAL 7 DAYS")).alias("reported_at_plus_7d"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_reported_at_plus_7d)).alias("days_since_prev_claim_date"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.ntile(10).over(w_all_by_reported_at_plus_7d).alias("reported_at_plus_7d_decile"),
        F.lag("reported_at_plus_7d", 2).over(w_all_by_reported_at_plus_7d).alias("prev2_reported_at_plus_7d"),
    )
)

sequenced = (
    metrics
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status_ref",
        "reported_at_plus_7d",
        "days_since_prev_claim_date",
        "rows_in_policy_id",
        "reported_at_plus_7d_decile",
        F.round(F.cume_dist().over(w_claim_type_by_reported_at_pl), 3).alias("reported_at_plus_7d_decile_cume_dist"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id_claim_type), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id_claim_type"),
        F.round(F.avg("days_since_prev_claim_date").over(w_claim_type_by_reported_at_pl_2), 2).alias("days_since_prev_claim_date_avg_2"),
    )
)

scored = (
    sequenced
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status_ref",
        "days_since_prev_claim_date",
        "rows_in_policy_id",
        "reported_at_plus_7d_decile",
        "claimed_amount_share_of_policy_id_claim_type",
        "days_since_prev_claim_date_avg_2",
        F.substring(F.trim(F.col("status_ref")), 1, 2).alias("status_ref_prefix"),
        F.when(F.col("rows_in_policy_id") >= 15, "HIGH").when(F.col("rows_in_policy_id") >= 5, "MEDIUM").otherwise("LOW").alias("rows_in_policy_id_tier"),
    )
)

sequenced_2 = (
    scored
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status_ref",
        "reported_at_plus_7d_decile",
        "claimed_amount_share_of_policy_id_claim_type",
        "days_since_prev_claim_date_avg_2",
        "rows_in_policy_id_tier",
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_reported_at_plus_7d_decile_p), 2).alias("claimed_amount_vs_reported_at_plus_7d_decile_pol"),
        F.rank().over(w_claim_type_by_claimed_amount).alias("claimed_amount_rank"),
    )
)

result = (
    sequenced_2
    .filter(F.col("claim_type").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status_ref",
        "reported_at_plus_7d_decile",
        "claimed_amount_share_of_policy_id_claim_type",
        "days_since_prev_claim_date_avg_2",
        "rows_in_policy_id_tier",
        "claimed_amount_rank",
    )
    .orderBy(F.col("claimed_amount").desc())
)
