from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status = Window.partitionBy("adjuster_id", "status")
w_status_claim_date_month_by_c = Window.partitionBy("status", F.date_trunc("month", "claim_date")).orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(10, 16)) & (F.year(F.col("claim_date")) == 2024))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

ranked = (
    cleaned
    .withColumn("claimed_amount_share_of_adjuster_id_status", F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id_status), F.lit(0)), 4))
    .withColumn("last_known_reported_at", F.last("reported_at", ignorenulls=True).over(w_status_claim_date_month_by_c))
)

scored = (
    ranked
    .select(
        "*",
        F.when(F.col("claimed_amount") >= 39666.5, "A").when(F.col("claimed_amount") >= 10518.4, "B").otherwise("C").alias("claimed_amount_tier"),
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
    )
)

labeled = (
    scored
    .filter(F.col("claim_date_label").isNotNull())
    .select(
        "*",
        F.abs(F.col("claimed_amount") - 27813.3).alias("claimed_amount_distance"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

tagged = (
    labeled
    .select(
        "*",
        F.round(F.col("claimed_amount_distance") * 0.25, 2).alias("claimed_amount_distance_adj"),
    )
)

exploded_detail_claims = (
    tagged.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.reported_at",
        "s.claimed_amount_tier",
        "s.claimed_amount_distance",
        "s.claimed_amount_per_approved_amount",
        "s.claimed_amount_distance_adj",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    exploded_detail_claims
    .groupBy("claim_claim_date")
    .pivot("claimed_amount_tier", ["C", "A", "B"])
    .agg(F.sum("claimed_amount_per_approved_amount"))
)
