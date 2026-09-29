from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())

valid_claims = (
    df_claims
    .filter(F.col("approved_amount") < 24718.5)
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
        F.concat_ws(": ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

deduped = (
    valid_claims
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

policy_id_stats = (
    deduped
    .groupBy("policy_id")
    .agg(F.max("claimed_amount").alias("max_claimed_amount"))
)

above_average = (
    deduped.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.claimed_amount") == F.col("g.max_claimed_amount"))
    .select(
        "s.*",
        "g.max_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.max_claimed_amount"), 2).alias("claimed_amount_minus_max"),
    )
)

result = (
    above_average
    .select(
        "id",
        F.col("claimed_amount_minus_max").alias("claimed_amount_minus_max_v"),
        F.col("max_claimed_amount").alias("max_claimed_amount_v"),
        F.col("claimed_amount").alias("m_claimed_amount"),
        F.col("approved_amount").alias("approved_amount_v"),
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount_minus_max_v", "max_claimed_amount_v", "m_claimed_amount", "approved_amount_v"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
