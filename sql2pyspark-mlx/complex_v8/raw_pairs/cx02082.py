from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status").orderBy("claimed_amount", "id")

cleaned_claims = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("claim_type") == "medical"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        F.concat_ws("|", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

qualified = (
    cleaned_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount") >= 6745.1), "left_anti")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

profiled = (
    qualified.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

derived = profiled.select("*", F.lower(F.trim(F.col("status"))).alias("status_clean"))

filtered = (
    derived.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.hour(F.col("c.reported_at")).between(8, 13)) & (F.col("c.approved_amount") <= 24718.5), "left_anti")
)

unique_rows = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "id",
        F.col("claim_count_2").alias("claim_count_2_v"),
        F.col("claim_count").alias("m_claim_count"),
    )
    .unpivot(
        ids=["id"],
        values=["claim_count_2_v", "m_claim_count"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .orderBy("id", "field")
)
