from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), F.col("id").desc())

cleaned_claims = (
    df_claims
    .filter(F.col("adjuster_id") >= 3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
    )
)

unique_rows = (
    cleaned_claims
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

derived = (
    unique_rows
    .filter(~F.col("claim_type").isin("theft", "medical"))
    .withColumn("claim_type_or_unknown", F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")))
)

result = (
    derived
    .rollup("policy_id", "status", "claim_type_or_unknown")
    .agg(
        F.grouping("policy_id").alias("is_total_policy_id"),
        F.count(F.when(F.col("claim_type") == "fire", 1)).alias("n_claim_type_fire"),
        F.count("*").alias("n_rows"),
    )
)
