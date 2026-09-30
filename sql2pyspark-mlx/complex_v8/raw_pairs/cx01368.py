from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_claim_approved = Window.partitionBy("claim_type").orderBy("claim_approved_amount", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_claim_approved_2 = Window.partitionBy("claim_type").orderBy("claim_approved_amount", "id", "claim_id")
w_claim_type_by_claim_approved_3 = Window.partitionBy("claim_type").orderBy("claim_approved_amount", "id", "claim_id").rowsBetween(-1, -1)
w_claim_id = Window.partitionBy("claim_id")

recent_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

with_children = (
    recent_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.status",
        "s.status_len",
        "s.status_clean",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

metrics = (
    with_children
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "status",
        "status_len",
        "claim_id",
        "claim_approved_amount",
        "claim_claim_type",
        "claim_claim_date",
        F.last("claim_date", ignorenulls=True).over(w_claim_type_by_claim_approved).alias("last_known_claim_date"),
        F.ntile(3).over(w_claim_type_by_claim_approved_2).alias("claim_approved_amount_tercile"),
        F.round(F.avg("status_len").over(w_claim_type_by_claim_approved_3), 2).alias("status_len_avg_1_prior"),
        F.sum("status_len").over(w_claim_id).alias("claim_id_sum_status_len"),
        (F.col("status_len") - F.lag("status_len").over(w_claim_type_by_claim_approved_2)).alias("status_len_change"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_type_by_claim_approved_2)).alias("days_since_prev_claim_date"),
    )
)

result = (
    metrics
    .groupingSets(
        [["claim_claim_type", "claim_type", "claim_approved_amount_tercile"], ["claim_claim_type"], ["claim_type"], []],
        "claim_claim_type", "claim_type", "claim_approved_amount_tercile",
    )
    .agg(
        F.sum(F.when(F.col("status") == "REJECTED", 1).otherwise(0)).alias("n_status_rejected"),
    )
    .orderBy(
        F.col("claim_claim_type").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
        F.col("claim_approved_amount_tercile").asc_nulls_last(),
    )
)
