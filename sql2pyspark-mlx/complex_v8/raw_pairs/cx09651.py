from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), F.col("id").desc())

filtered = (
    df_claims
    .filter(F.col("adjuster_id") <= 6)
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
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

with_details_policy = (
    filtered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select("s.*", F.col("p.start_date").alias("policy_start_date"))
)

latest = (
    with_details_policy
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest.alias("s")
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
        "s.status_len",
        "s.claim_type_clean",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    enriched
    .groupBy("adjuster_id", "claim_date")
    .pivot("claim_type", ["theft", "medical", "flood", "collision"])
    .agg(F.count("approved_amount"))
    .orderBy("adjuster_id", "claim_date")
)
