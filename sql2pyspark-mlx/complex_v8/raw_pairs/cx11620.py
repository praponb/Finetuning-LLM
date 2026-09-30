from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids_n_rows = Window.partitionBy("distinct_ids", "n_rows")
w_distinct_holder_segments_by = Window.partitionBy("distinct_holder_segments").orderBy(F.col("n_rows").desc(), "holder_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_holder_segments_by_2 = Window.partitionBy("distinct_holder_segments").orderBy(F.col("distinct_ids").asc_nulls_last())
w_distinct_holder_segments_by_3 = Window.partitionBy("distinct_holder_segments").orderBy(F.col("n_rows").desc(), "holder_id")

base = (
    df_policies
    .filter((F.col("premium") > 2111.3) & (F.col("end_date").between(F.lit("2025-11-20").cast("date"), F.lit("2026-10-23").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
    )
)

with_counts = (
    base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.last_claim_reported_at", "c.max_claim_claimed_amount")
)

annotated = (
    with_counts
    .filter(F.col("premium") > 1132.1)
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.round(F.col("premium") / F.nullif(F.col("max_claim_claimed_amount"), F.lit(0)), 3).alias("premium_per_max_claim_claimed_amount"),
    )
)

combined_details = (
    annotated.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.days_end_date_to_start_date",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

grouped = (
    combined_details
    .filter(F.col("holder_joined_on") >= F.lit("2024-07-18").cast("date"))
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("holder_segment").alias("distinct_holder_segments"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

metrics = (
    grouped
    .select(
        "holder_id",
        "n_rows",
        "distinct_holder_segments",
        "distinct_ids",
        F.min("n_rows").over(w_distinct_ids_n_rows).alias("distinct_ids_n_rows_min_n_rows"),
        F.round(F.col("distinct_holder_segments") - F.avg("distinct_holder_segments").over(w_distinct_ids_n_rows), 2).alias("distinct_holder_segments_vs_distinct_ids_n_rows_"),
        F.last("distinct_holder_segments").over(w_distinct_holder_segments_by).alias("last_distinct_holder_segments"),
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_distinct_ids_n_rows), 2).alias("n_rows_vs_distinct_ids_n_rows_avg"),
        F.round(F.percent_rank().over(w_distinct_holder_segments_by_2), 3).alias("distinct_ids_percent_rank"),
        (F.col("distinct_holder_segments") - F.lag("distinct_holder_segments").over(w_distinct_holder_segments_by_3)).alias("distinct_holder_segments_change"),
    )
)

with_activity = (
    metrics
    .crossJoin(metrics.agg(F.min("distinct_ids").alias("overall_min_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_min_distinct_ids"))
    .drop("overall_min_distinct_ids")
)

result = (
    with_activity
    .select(
        "holder_id",
        "n_rows_vs_distinct_ids_n_rows_avg",
        "distinct_ids_percent_rank",
    )
    .unpivot(
        ids=["holder_id"],
        values=["n_rows_vs_distinct_ids_n_rows_avg", "distinct_ids_percent_rank"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
