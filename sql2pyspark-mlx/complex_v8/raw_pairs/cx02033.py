from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product", "status").orderBy("last_claim_reported_at", "id")

base_policys = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("end_date") < F.lit("2026-09-19").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.abs(F.col("premium") - 2824.5).alias("premium_distance"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("claim_claim_type_count"),
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count("*").alias("claim_count"),
    )
)

with_stats = (
    base_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

latest_per_key = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .groupingSets(
        [["status", "holder_id", "claim_claim_type_count"], ["status"], ["holder_id"]],
        "status", "holder_id", "claim_claim_type_count",
    )
    .agg(F.max("premium_distance").alias("max_premium_distance"))
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "holder_id",
        "claim_claim_type_count",
        "max_premium_distance",
    )
)
