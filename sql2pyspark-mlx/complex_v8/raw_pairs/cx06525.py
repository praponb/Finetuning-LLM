from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_approved_amount = Window.orderBy(F.col("claim_approved_amount").desc_nulls_first(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_approved_amount_2 = Window.orderBy(F.col("claim_approved_amount").desc_nulls_first(), "id", "claim_id")

valid_claims = (
    df_claims
    .filter((F.to_date(F.col("reported_at")) < F.lit("2025-03-14").cast("date")) & (F.col("claim_type") == "fire"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.when(F.col("approved_amount") >= 19337.6, "HIGH").when(F.col("approved_amount") >= 1273.7, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
        F.year(F.col("claim_date")).alias("claim_date_year"),
    )
)

detail_rows_claims = (
    valid_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.reported_at") >= F.lit("2025-01-31 14:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.reported_at",
        "s.approved_amount_tier",
        "s.claim_date_year",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

windowed = (
    detail_rows_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "approved_amount_tier",
        "claim_id",
        "claim_claim_date",
        "claim_approved_amount",
        F.sum("claim_date_year").over(w_all_by_claim_approved_amount).alias("running_claim_date_year"),
        F.ntile(10).over(w_all_by_claim_approved_amount_2).alias("claim_approved_amount_decile"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

summarized_rows = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.avg_claim_claimed_amount", "c.first_claim_reported_at")
)

grouped = (
    summarized_rows
    .groupBy(
        F.year("first_claim_reported_at").alias("first_claim_reported_at_year"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.max("avg_claim_claimed_amount").alias("max_avg_claim_claimed_amount"),
        F.percentile_approx("running_claim_date_year", 0.5).alias("median_running_claim_date_year"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.datediff(F.max("claim_claim_date"), F.min("claim_claim_date")).alias("claim_claim_date_span_days"),
    )
)

scored = (
    grouped
    .select(
        "*",
        F.abs(F.col("n_rows") - 9).alias("n_rows_distance"),
        (F.col("distinct_adjuster_ids") + F.col("n_rows")).alias("distinct_adjuster_ids_plus_n_rows"),
    )
)

result = (
    scored
    .select(
        "first_claim_reported_at_year",
        "n_rows",
        "distinct_policy_ids",
        "max_avg_claim_claimed_amount",
        "distinct_adjuster_ids",
        "n_rows_distance",
        "distinct_adjuster_ids_plus_n_rows",
    )
    .orderBy(F.col("n_rows").asc(), F.col("first_claim_reported_at_year").desc())
)
