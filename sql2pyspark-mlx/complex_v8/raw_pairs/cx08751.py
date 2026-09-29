from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("least_approved_amount_claimed_amount").desc(), F.col("id").desc())

cleaned_claims = (
    df_claims
    .filter((F.col("claim_type").isNotNull()) & (F.col("approved_amount") <= 14595.6))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

deduped = (
    cleaned_claims
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .cube("status", "adjuster_id", "policy_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("reported_at").alias("last_reported_at"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "adjuster_id",
        "policy_id",
        "n_rows",
        "last_reported_at",
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
