from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_adjuster_id_by_claime = Window.partitionBy("status", "adjuster_id").orderBy(F.col("claimed_amount_distance").desc(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_status_adjuster_id_by_claime_2 = Window.partitionBy("status", "adjuster_id").orderBy(F.col("claimed_amount_distance").desc())

base = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        (F.col("reported_at") + F.expr("INTERVAL 1 DAYS")).alias("reported_at_plus_1d"),
        F.add_months(F.col("claim_date"), 6).alias("claim_date_plus_6m"),
        F.abs(F.col("claimed_amount") - 20505.7).alias("claimed_amount_distance"),
    )
)

metrics = (
    base
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "reported_at_plus_1d",
        "claim_date_plus_6m",
        "claimed_amount_distance",
        F.lead("claim_date_plus_6m").over(w_status_adjuster_id_by_claime).alias("next_claim_date_plus_6m"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id"),
        F.round(F.cume_dist().over(w_status_adjuster_id_by_claime_2), 3).alias("claimed_amount_distance_cume_dist"),
    )
)

result = (
    metrics
    .groupingSets(
        [["status", "policy_id", "adjuster_id"], ["status"]],
        "status", "policy_id", "adjuster_id",
    )
    .agg(F.count("*").alias("row_count"))
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
