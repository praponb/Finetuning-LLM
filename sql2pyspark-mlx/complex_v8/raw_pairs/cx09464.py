from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("product").orderBy("end_date", "id")

cleaned = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-07-19").cast("date"), F.lit("2025-03-24").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.round(F.col("premium") * 0.9, 2).alias("premium_adj"),
        F.concat_ws("|", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.sum("claimed_amount").alias("total_claim_claimed_amount"))
)

summarized_rows = (
    latest.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.total_claim_claimed_amount")
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.min("claim_date").alias("first_claim_date"),
        F.count("*").alias("claim_count"),
    )
)

with_stats = (
    summarized_rows.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        "c.first_claim_date",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    with_stats
    .groupingSets(
        [["status", "holder_id", "product"], ["status"]],
        "status", "holder_id", "product",
    )
    .agg(F.round(F.sum("premium_adj"), 2).alias("total_premium_adj"))
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
    )
)
