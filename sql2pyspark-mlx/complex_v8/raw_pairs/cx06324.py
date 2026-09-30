from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claimed_amount_tier").orderBy(F.col("approved_amount").desc(), F.col("id").desc())
w_adjuster_id_by_claim_approve = Window.partitionBy("adjuster_id").orderBy(F.col("claim_approved_amount").asc_nulls_last(), "id", "claim_id", "claim_id_2")
w_adjuster_id_by_claim_approve_2 = Window.partitionBy("adjuster_id").orderBy(F.col("claim_approved_amount").asc_nulls_last(), "id", "claim_id", "claim_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_claims = (
    df_claims
    .filter(F.col("approved_amount") <= 14668.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

tagged = (
    valid_claims
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.when(F.col("claimed_amount") >= 39666.5, "A").when(F.col("claimed_amount") >= 17759.3, "B").otherwise("C").alias("claimed_amount_tier"),
    )
)

deduped = (
    tagged
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

exploded_detail = (
    deduped.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") >= 2272.7), "left")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount_tier",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

exploded_detail_claims = (
    exploded_detail.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.claim_id",
        "s.claim_claim_type",
        "s.claim_approved_amount",
        F.col("c.id").alias("claim_id_2"),
        "c.claim_date",
        F.col("c.claim_type").alias("claim_claim_type_2"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

sequenced = (
    exploded_detail_claims
    .select(
        "id",
        "adjuster_id",
        "claim_id",
        "claim_claim_type",
        "claim_approved_amount",
        "claim_id_2",
        "claim_date",
        "claim_claim_type_2",
        "claim_claimed_amount",
        F.lag("claim_claimed_amount", 3, 0).over(w_adjuster_id_by_claim_approve).alias("prev3_claim_claimed_amount"),
        (F.col("claim_approved_amount") - F.lag("claim_approved_amount").over(w_adjuster_id_by_claim_approve)).alias("claim_approved_amount_change"),
        F.max("claim_approved_amount").over(w_adjuster_id_by_claim_approve_2).alias("max_claim_approved_amount_so_far"),
    )
)

result = (
    sequenced
    .groupBy("claim_id", "claim_id_2")
    .pivot("claim_claim_type", ["delay", "fire", "theft"])
    .agg(F.count("claim_claimed_amount"))
    .orderBy("claim_id", "claim_id_2")
)
