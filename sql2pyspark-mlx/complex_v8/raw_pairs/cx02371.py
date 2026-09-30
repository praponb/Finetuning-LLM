from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy("claim_date", "id")
w_status = Window.partitionBy("status")
w_policy_id_by_reported_at_run = Window.partitionBy("policy_id").orderBy(F.col("reported_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_claims
    .filter((F.col("status").isin("APPROVED", "OPEN")) & (F.col("approved_amount") >= 5336.7))
)

ranked = (
    recent
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.lead("claimed_amount").over(w_policy_id_by_claim_date).alias("next_claimed_amount"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_policy_id_by_claim_date)).alias("days_since_prev_claim_date"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.sum("claimed_amount").over(w_policy_id_by_reported_at_run).alias("running_claimed_amount"),
    )
)

claim_type_stats = (
    ranked
    .groupBy("claim_type")
    .agg(F.avg("days_since_prev_claim_date").alias("avg_days_since_prev_claim_date"))
)

compared = (
    ranked.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.days_since_prev_claim_date") > F.col("g.avg_days_since_prev_claim_date"))
    .select(
        "s.*",
        "g.avg_days_since_prev_claim_date",
        F.round(F.col("s.days_since_prev_claim_date") - F.col("g.avg_days_since_prev_claim_date"), 2).alias("days_since_prev_claim_date_minus_avg"),
    )
)

labeled = (
    compared
    .select(
        "*",
        (F.col("rows_in_status") - F.col("claimed_amount")).alias("rows_in_status_minus_claimed_amount"),
        F.round(F.col("running_claimed_amount") / F.nullif(F.col("next_claimed_amount"), F.lit(0)), 3).alias("running_claimed_amount_per_next_claimed_amount"),
        (F.floor(F.col("running_claimed_amount") / 100000) * 100000).alias("running_claimed_amount_bucket"),
    )
)

result = (
    labeled
    .select("reported_at", "running_claimed_amount_bucket", "claim_type")
    .distinct()
    .orderBy(F.col("running_claimed_amount_bucket").desc_nulls_first())
)
