from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_type", "adjuster_id").orderBy(F.col("reported_at").desc(), F.col("id").desc())

recent_claims = (
    df_claims
    .filter((F.col("claimed_amount") <= 32796.4) & (F.to_date(F.col("reported_at")) < F.lit("2025-03-14").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
    )
)

joined_policy = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status") == "EXPIRED"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.reported_at",
        "s.claim_date_label",
        F.col("p.premium").alias("policy_premium"),
    )
)

latest = (
    joined_policy
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        "policy_premium",
    )
    .orderBy(F.col("claim_date").desc(), F.col("policy_premium").desc(), "id")
    .limit(20)
)
