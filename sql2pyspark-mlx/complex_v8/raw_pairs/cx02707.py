from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")

prepared_claims = (
    df_claims
    .filter(F.col("claimed_amount") >= 4868.7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

sequenced = (
    prepared_claims
    .withColumn("claimed_amount_share_of_claim_type_adjuster_id", F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_type_adjuster_id), F.lit(0)), 4))
    .withColumn("distinct_policy_id_in_claim_type_adjuster_id", F.size(F.collect_set("policy_id").over(w_claim_type_adjuster_id)))
)

result = (
    sequenced
    .select(
        "id",
        F.col("claimed_amount_share_of_claim_type_adjuster_id").alias("claimed_amount_share_of_claim_type_adjuster_id_v"),
        F.col("claimed_amount").alias("m_claimed_amount"),
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount_share_of_claim_type_adjuster_id_v", "m_claimed_amount"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
