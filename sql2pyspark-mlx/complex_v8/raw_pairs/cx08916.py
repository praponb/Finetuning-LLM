from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_status_by_rows_in_status_run = Window.partitionBy("status").orderBy(F.col("rows_in_status").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_status_by_claimed = Window.partitionBy("claim_type", "status").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        (F.col("reported_at") + F.expr("INTERVAL 30 MINUTES")).alias("reported_at_plus_30m"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.size(F.collect_set("adjuster_id").over(w_status)).alias("distinct_adjuster_id_in_status"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status), F.lit(0)), 4).alias("claimed_amount_share_of_status"),
        F.min("claimed_amount").over(w_status).alias("status_min_claimed_amount"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at_plus_30m",
        "rows_in_status",
        "distinct_adjuster_id_in_status",
        "claimed_amount_share_of_status",
        F.first("reported_at", ignorenulls=True).over(w_status_by_rows_in_status_run).alias("first_known_reported_at"),
        F.first("claimed_amount").over(w_claim_type_status_by_claimed).alias("first_claimed_amount"),
    )
)

adjuster_id_stats = (
    with_windows
    .groupBy("adjuster_id")
    .agg(F.avg("claimed_amount_share_of_status").alias("avg_claimed_amount_share_of_status"))
)

above_average = (
    with_windows.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.claimed_amount_share_of_status") > F.col("g.avg_claimed_amount_share_of_status"))
    .select(
        "s.*",
        "g.avg_claimed_amount_share_of_status",
        F.round(F.col("s.claimed_amount_share_of_status") - F.col("g.avg_claimed_amount_share_of_status"), 2).alias("claimed_amount_share_of_status_minus_avg"),
    )
)

with_activity = (
    above_average.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status").isin("OPEN", "APPROVED")), "left_anti")
    .filter(F.col("adjuster_id") <= 5)
)

result = (
    with_activity
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "rows_in_status",
        "distinct_adjuster_id_in_status",
        "claimed_amount_share_of_status",
        "first_known_reported_at",
        "first_claimed_amount",
        "avg_claimed_amount_share_of_status",
        "claimed_amount_share_of_status_minus_avg",
    )
    .orderBy(F.col("approved_amount").desc_nulls_first())
)
