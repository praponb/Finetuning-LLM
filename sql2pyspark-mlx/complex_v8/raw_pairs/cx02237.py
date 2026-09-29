from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_claim_type_by_last_claim_rep = Window.partitionBy("claim_type").orderBy(F.col("last_claim_reported_at").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_claim_type_by_last_claim_rep_2 = Window.partitionBy("claim_type").orderBy(F.col("last_claim_reported_at").asc_nulls_last(), "id")

valid_claims = (
    df_claims
    .filter((F.col("claimed_amount") < 40638.4) & (F.col("claim_date") >= F.lit("2024-08-10").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

with_stats = (
    valid_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        "c.avg_claim_approved_amount",
        "c.last_claim_date",
    )
)

sequenced = (
    with_stats
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "claim_type_prefix",
        "last_claim_reported_at",
        "avg_claim_approved_amount",
        "last_claim_date",
        F.size(F.collect_set("claim_type").over(w_policy_id)).alias("distinct_claim_type_in_policy_id"),
        F.max("claimed_amount").over(w_claim_type_by_last_claim_rep).alias("claimed_amount_max_7"),
        F.rank().over(w_claim_type_by_last_claim_rep_2).alias("last_claim_reported_at_rank"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id), 2).alias("approved_amount_vs_policy_id_avg"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["adjuster_id", "distinct_claim_type_in_policy_id", "policy_id"], ["adjuster_id"], ["distinct_claim_type_in_policy_id"]],
        "adjuster_id", "distinct_claim_type_in_policy_id", "policy_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("claimed_amount").alias("max_claimed_amount"),
        F.datediff(F.max("last_claim_date"), F.min("last_claim_date")).alias("last_claim_date_span_days"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("distinct_claim_type_in_policy_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
