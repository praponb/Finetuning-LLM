from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type_by_clai = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("claim_date").desc(), "id")
w_adjuster_id_claim_type = Window.partitionBy("adjuster_id", "claim_type")
w_policy_id_claim_type_by_clai_2 = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("claim_date").desc())
w_policy_id_claim_type_by_clai_3 = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("claim_date").desc(), "id").rowsBetween(-6, -1)
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy(F.col("approved_amount_filled").desc(), "id")

prepared = (
    df_claims
    .filter((F.col("claimed_amount").between(4698.6, 40639.7)) & (F.col("approved_amount") >= 0.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.date_format(F.col("reported_at"), "yyyy-MM-dd").alias("reported_at_label"),
    )
)

derived = (
    prepared
    .withColumn("reported_at_label_ref", F.concat(F.col("reported_at_label"), F.lit("#"), F.col("id").cast("string")))
)

windowed = (
    derived
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "approved_amount_filled",
        "reported_at_label_ref",
        F.lag("approved_amount_filled").over(w_policy_id_claim_type_by_clai).alias("prev_approved_amount_filled"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id_claim_type), 2).alias("claimed_amount_vs_adjuster_id_claim_type_avg"),
        F.rank().over(w_policy_id_claim_type_by_clai_2).alias("claim_date_rank"),
        F.sum("approved_amount_filled").over(w_policy_id_claim_type_by_clai_3).alias("approved_amount_filled_sum_6_prior"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_type_by_approved_amoun)).alias("days_since_prev_claim_date"),
        F.max("approved_amount_filled").over(w_adjuster_id_claim_type).alias("adjuster_id_claim_type_max_approved_amount_fille"),
    )
)

with_children_claims = (
    windowed.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (~F.col("c.claim_type").isin("flood", "medical")), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.reported_at",
        "s.reported_at_label_ref",
        "s.prev_approved_amount_filled",
        "s.days_since_prev_claim_date",
        "s.adjuster_id_claim_type_max_approved_amount_fille",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

result = (
    with_children_claims
    .groupBy("claim_id")
    .pivot("claim_claim_type", ["flood", "collision", "medical"])
    .agg(F.count("days_since_prev_claim_date"))
    .orderBy("claim_id")
)
