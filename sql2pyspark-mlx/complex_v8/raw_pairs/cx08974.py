from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("product").orderBy("approved_amount_distance", "id")

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

scored = (
    joined_ranges
    .filter(F.col("product").isin("LIFE", "TRAVEL"))
    .select(
        "*",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.abs(F.col("approved_amount") - 5336.7).alias("approved_amount_distance"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

with_stats = (
    scored.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.last_claim_reported_at", "c.first_claim_reported_at")
)

unique_rows = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .groupBy("policy_id")
    .pivot("claim_type", ["theft", "delay", "flood", "collision"])
    .agg(F.sum("premium_filled"))
    .orderBy("policy_id")
)
