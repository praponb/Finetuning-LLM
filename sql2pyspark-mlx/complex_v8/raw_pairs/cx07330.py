from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_adjuster_id_by_day = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("days_since_reported_at").desc(), "id")
w_all_by_claim_date = Window.orderBy("claim_date", "id")

scoped_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
    )
)

with_trends = (
    scoped_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "days_since_reported_at",
        "claim_type_ref",
        F.row_number().over(w_policy_id_adjuster_id_by_day).alias("rn"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_claim_date)).alias("days_since_prev_claim_date"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["rn", "policy_id", "adjuster_id"], ["rn"]],
        "rn", "policy_id", "adjuster_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.max("claim_date").alias("last_claim_date"),
        F.max("days_since_reported_at").alias("max_days_since_reported_at"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("rn").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
