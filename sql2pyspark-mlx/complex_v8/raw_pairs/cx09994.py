from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status = Window.partitionBy("adjuster_id", "status")
w_adjuster_id_policy_id_by_cla = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_policy_id_by_dis = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("distinct_policy_id_in_adjuster_id_status").desc(), "id", "claim_id").rowsBetween(-6, Window.currentRow)
w_policy_id = Window.partitionBy("policy_id")

cleaned_claims = (
    df_claims
    .filter((F.col("approved_amount").between(0.0, 20843.1)) | (F.col("policy_id") <= 10))
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
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

sequenced = (
    cleaned_claims
    .withColumn("distinct_policy_id_in_adjuster_id_status", F.size(F.collect_set("policy_id").over(w_adjuster_id_status)))
    .withColumn("first_known_claimed_amount", F.first("claimed_amount", ignorenulls=True).over(w_adjuster_id_policy_id_by_cla))
)

detail_rows = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.status",
        "s.distinct_policy_id_in_adjuster_id_status",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

annotated = (
    detail_rows
    .select(
        "*",
        F.when(F.col("status").isin("OPEN", "PAID"), "group_a").when(F.col("status") == "REJECTED", "group_b").otherwise("other").alias("status_group"),
        F.least(F.col("claim_approved_amount"), F.col("claim_claimed_amount")).alias("least_claim_approved_amount_claim_claimed_amount"),
    )
)

with_trends = (
    annotated
    .select(
        "*",
        F.round(F.avg("claim_claimed_amount").over(w_adjuster_id_policy_id_by_dis), 2).alias("claim_claimed_amount_avg_7"),
        F.round(F.col("claim_claimed_amount") / F.nullif(F.sum("claim_claimed_amount").over(w_policy_id), F.lit(0)), 4).alias("claim_claimed_amount_share_of_policy_id"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
    )
)

result = (
    with_trends
    .filter(F.col("claim_claimed_amount") <= 42447.2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "distinct_policy_id_in_adjuster_id_status",
        "claim_id",
        "claim_approved_amount",
        "claim_status",
        "status_group",
        "least_claim_approved_amount_claim_claimed_amount",
        "claim_claimed_amount_share_of_policy_id",
        "rows_in_policy_id",
    )
    .orderBy(F.col("id").desc())
)
