from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product").orderBy("claim_status_count", "id")

covered = (
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
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

adjuster_id_stats = (
    covered
    .groupBy("adjuster_id")
    .agg(F.avg("claimed_amount").alias("avg_claimed_amount"))
)

outliers = (
    covered.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.claimed_amount") > F.col("g.avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.avg_claimed_amount"), 2).alias("claimed_amount_minus_avg"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(~F.col("status").isin("REJECTED", "OPEN"), 1)).alias("claim_status_count"),
    )
)

summarized_rows = (
    outliers.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

unique_rows = (
    summarized_rows
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "adjuster_id",
        "product",
        "premium",
        "claim_status_count",
    )
    .orderBy(F.col("claimed_amount").desc_nulls_first())
)
