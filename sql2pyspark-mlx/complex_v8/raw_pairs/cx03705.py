from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_team").orderBy("claim_type_len", "id")

valid_claims = df_claims.filter(F.col("status") != "OPEN")

tagged = (
    valid_claims
    .select(
        "*",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

combined_details_policy = (
    tagged.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_type_len",
        F.col("p.start_date").alias("policy_start_date"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

deduped = (
    combined_details_policy
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .cube("claim_type_len", "adjuster_id", "policy_id")
    .agg(
        F.round(F.stddev("approved_amount"), 2).alias("stddev_approved_amount"),
    )
    .orderBy(
        F.col("claim_type_len").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
