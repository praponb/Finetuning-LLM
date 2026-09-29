from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("claim_policy_id").orderBy(F.col("claim_approved_amount").desc(), F.col("id").desc(), F.col("claim_id").desc())

cleaned = (
    df_adjusters
    .filter(F.col("team") != "Travel")
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

detail_rows = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.adjuster_name",
        "s.team",
        "s.level",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_type",
    )
)

deduped = (
    detail_rows
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .select(
        "id",
        "claim_id",
        "claim_claimed_amount",
        "claim_approved_amount",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["claim_claimed_amount", "claim_approved_amount"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
