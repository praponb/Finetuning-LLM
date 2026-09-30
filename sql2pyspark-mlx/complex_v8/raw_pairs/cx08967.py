from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_reported_at_we = Window.partitionBy("claim_type").orderBy(F.col("reported_at_week").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type = Window.partitionBy("claim_type")
w_reported_at_week_month_by_re = Window.partitionBy(F.month("reported_at_week")).orderBy("reported_at", "id")
w_claim_type_by_reported_at_we_2 = Window.partitionBy("claim_type").orderBy(F.col("reported_at_week").desc())
w_claim_type_by_reported_at_we_3 = Window.partitionBy("claim_type").orderBy(F.col("reported_at_week").desc(), "id")

filtered = (
    df_claims
    .filter((F.col("claimed_amount").between(5313.8, 42838.9)) & (F.col("claim_type").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
        F.month(F.col("reported_at")).alias("reported_at_month"),
        F.date_trunc("week", F.col("reported_at")).alias("reported_at_week"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (~F.col("c.claim_type").isin("delay", "medical")) & (F.col("c.claim_date").between(F.lit("2024-03-24").cast("date"), F.lit("2025-11-08").cast("date"))), "left_anti")
)

with_windows = (
    screened
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_type_by_reported_at_we).alias("running_count"),
        F.round(F.col("reported_at_month") / F.nullif(F.sum("reported_at_month").over(w_claim_type), F.lit(0)), 4).alias("reported_at_month_share_of_claim_type"),
        F.row_number().over(w_reported_at_week_month_by_re).alias("rn"),
        F.round(F.percent_rank().over(w_claim_type_by_reported_at_we_2), 3).alias("reported_at_week_percent_rank"),
        F.row_number().over(w_claim_type_by_reported_at_we_3).alias("seq_no"),
        F.ntile(4).over(w_reported_at_week_month_by_re).alias("reported_at_quartile"),
    )
)

status_stats = (
    with_windows
    .groupBy("status")
    .agg(F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"))
)

above_average = (
    with_windows.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.approved_amount") >= F.col("g.median_approved_amount"))
    .select(
        "s.*",
        "g.median_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.median_approved_amount"), 2).alias("approved_amount_minus_median"),
    )
)

result = (
    above_average
    .select(
        "id",
        F.col("reported_at_quartile").alias("m_reported_at_quartile"),
        F.col("running_count").alias("m_running_count"),
        F.col("reported_at_month").alias("reported_at_month_v"),
        F.col("seq_no").alias("seq_no_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_reported_at_quartile", "m_running_count", "reported_at_month_v", "seq_no_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
