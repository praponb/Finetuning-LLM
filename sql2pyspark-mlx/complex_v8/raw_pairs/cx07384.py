from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status_by_premium = Window.partitionBy("holder_id", "status").orderBy(F.col("premium").desc(), "id")
w_product_by_premium_running = Window.partitionBy("product").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("end_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.when(F.col("status").isin("LAPSED", "CANCELLED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
    )
)

sequenced = (
    recent
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.lag("premium", 2).over(w_holder_id_status_by_premium).alias("prev2_premium"),
        F.last("premium", ignorenulls=True).over(w_product_by_premium_running).alias("last_known_premium"),
    )
)

with_details_holder = (
    sequenced.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
    )
)

exploded_detail_claims = (
    with_details_holder.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        "s.prev2_premium",
        "s.holder_risk_score",
        "s.holder_name",
        "s.holder_segment",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

derived = (
    exploded_detail_claims
    .select(
        "id",
        "product",
        "end_date",
        "status",
        "holder_risk_score",
        "holder_segment",
        "claim_id",
        "claim_reported_at",
        "claim_claimed_amount",
        F.substring(F.trim(F.col("holder_segment")), 1, 3).alias("holder_segment_prefix"),
        F.date_format(F.col("end_date"), "MMM yyyy").alias("end_date_label"),
        F.add_months(F.col("end_date"), 3).alias("end_date_plus_3m"),
    )
)

qualified = (
    derived
    .crossJoin(derived.agg(F.avg("holder_risk_score").alias("overall_avg_holder_risk_score")))
    .filter(F.col("holder_risk_score") > F.col("overall_avg_holder_risk_score"))
    .drop("overall_avg_holder_risk_score")
)

joined = (
    qualified.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (F.col("c.claim_date") < F.lit("2025-06-24").cast("date")), "inner")
    .filter(F.col("c.adjuster_id").isNotNull())
    .select(
        "s.id",
        "s.end_date",
        "s.status",
        "s.claim_id",
        "s.claim_reported_at",
        "s.claim_claimed_amount",
        "s.holder_segment_prefix",
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

result = (
    joined
    .cube("claim_adjuster_id", "status", "claim_id")
    .agg(
        F.sum(F.when(F.col("claim_status") == "REJECTED", F.col("claim_claimed_amount")).otherwise(0)).alias("claim_claimed_amount_claim_status_rejected"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("claim_adjuster_id").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
    )
)
