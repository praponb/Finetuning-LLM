from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_reported_at_ful = Window.partitionBy("policy_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id = Window.partitionBy("policy_id")
w_status_by_claim_count_runnin = Window.partitionBy("status").orderBy("claim_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_first_reported_at_run = Window.orderBy("first_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status = Window.partitionBy("status")
w_all_by_first_reported_at_ful = Window.orderBy("first_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_claim_count = Window.partitionBy("status", "claim_count")
w_first_reported_at_to_date_ad = Window.partitionBy(F.to_date("first_reported_at"), "adjuster_id").orderBy("policy_id_max_claimed_amount", "id")
w_first_reported_at_month_last = Window.partitionBy(F.date_trunc("month", "first_reported_at"), "last_claim_count").orderBy(F.col("last_claim_count").desc(), "id")

scoped_claims = (
    df_claims
    .filter((F.col("approved_amount") < 14668.0) & (F.col("policy_id") >= 5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

with_activity = (
    scoped_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") >= F.lit("2025-01-24").cast("date")), "left_semi")
)

sequenced = (
    with_activity
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.first("reported_at").over(w_policy_id_by_reported_at_ful).alias("first_reported_at"),
        F.max("claimed_amount").over(w_policy_id).alias("policy_id_max_claimed_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("approved_amount").alias("min_claim_approved_amount"),
        F.count("*").alias("claim_count"),
    )
)

profiled = (
    sequenced.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.min_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

with_trends = (
    profiled
    .select(
        "*",
        F.count(F.lit(1)).over(w_status_by_claim_count_runnin).alias("running_count"),
        F.min("claim_count").over(w_all_by_first_reported_at_run).alias("min_claim_count_so_far"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status), 2).alias("claimed_amount_vs_status_avg"),
        F.last("claim_count").over(w_all_by_first_reported_at_ful).alias("last_claim_count"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
        "first_reported_at",
        "policy_id_max_claimed_amount",
        "claim_count",
        "min_claim_count_so_far",
        F.max("policy_id_max_claimed_amount").over(w_status_claim_count).alias("status_claim_count_max_policy_id_max_claimed_amo"),
        F.lead("claim_count", 3).over(w_first_reported_at_to_date_ad).alias("next3_claim_count"),
        F.count(F.lit(1)).over(w_status_claim_count).alias("rows_in_status_claim_count"),
        F.lead("claimed_amount", 3, 0).over(w_first_reported_at_month_last).alias("next3_claimed_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "status",
        "first_reported_at",
        "policy_id_max_claimed_amount",
        "min_claim_count_so_far",
        "status_claim_count_max_policy_id_max_claimed_amo",
        "next3_claim_count",
        "rows_in_status_claim_count",
        "next3_claimed_amount",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("min_claim_count_so_far").cast("string")).alias("claim_type_ref"),
        F.to_date(F.col("first_reported_at")).alias("first_reported_at_date"),
    )
    .orderBy(
        F.col("status_claim_count_max_policy_id_max_claimed_amo").desc(),
        F.col("next3_claim_count").desc(),
    )
)
