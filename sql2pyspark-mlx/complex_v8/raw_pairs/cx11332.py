from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount_runnin = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_status = Window.partitionBy("adjuster_id", "status")
w_dedup = Window.partitionBy("status").orderBy(F.col("claim_date").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("claim_type").orderBy(F.col("claim_date").desc(), F.col("id").desc())

valid = df_claims.filter(F.col("status").isin("APPROVED", "IN_REVIEW"))

with_trends = (
    valid
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.last("approved_amount", ignorenulls=True).over(w_all_by_claimed_amount_runnin).alias("last_known_approved_amount"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id_status)).alias("distinct_policy_id_in_adjuster_id_status"),
    )
)

deduped = (
    with_trends
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

unique_rows = (
    deduped
    .withColumn("rn", F.row_number().over(w_dedup_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined = (
    unique_rows.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.last_known_approved_amount",
        "s.distinct_policy_id_in_adjuster_id_status",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.count(F.when(F.col("status").isin("APPROVED", "PAID"), 1)).alias("claim_status_count"),
    )
)

with_stats = (
    joined.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.first_claim_date",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

result = (
    with_stats
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "claim_type",
        "claimed_amount",
        "last_known_approved_amount",
        "adjuster_team",
        "first_claim_date",
        F.greatest(F.col("claim_status_count"), F.col("adjuster_level")).alias("greatest_claim_status_count_adjuster_level"),
        F.round(F.col("claimed_amount") * 0.25, 2).alias("claimed_amount_adj"),
    )
    .orderBy("claimed_amount_adj", F.col("first_claim_date").asc(), "id")
    .limit(5)
)
