from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_to_date = Window.partitionBy(F.to_date("reported_at"))
w_all_by_claimed_amount_runnin = Window.orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claimed_amount_full = Window.orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
    )
)

sequenced = (
    scoped_claims
    .select(
        "id",
        "policy_id",
        "reported_at",
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_reported_at_to_date), F.lit(0)), 4).alias("claimed_amount_share_of_reported_at_to_date"),
        F.last("reported_at", ignorenulls=True).over(w_all_by_claimed_amount_runnin).alias("last_known_reported_at"),
        F.last("claimed_amount").over(w_all_by_claimed_amount_full).alias("last_claimed_amount"),
        F.count(F.lit(1)).over(w_reported_at_to_date).alias("rows_in_reported_at_to_date"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_reported_at_to_date), 2).alias("claimed_amount_vs_reported_at_to_date_avg"),
        F.max("claimed_amount").over(w_all_by_claimed_amount_runnin).alias("max_claimed_amount_so_far"),
    )
)

with_parent_policy = (
    sequenced.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.reported_at",
        "s.last_known_reported_at",
        "s.last_claimed_amount",
        "s.rows_in_reported_at_to_date",
        "s.claimed_amount_vs_reported_at_to_date_avg",
        "s.max_claimed_amount_so_far",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    with_parent_policy
    .rollup("policy_id", "policy_status", "policy_holder_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("policy_status")).alias("policy_status_list"),
        F.round(F.stddev("claimed_amount_vs_reported_at_to_date_avg"), 2).alias("stddev_claimed_amount_vs_reported_at_to_date_avg"),
        F.round(100 * F.count(F.when(~F.col("policy_status").isin("CANCELLED", "EXPIRED"), 1)) / F.count("*"), 1).alias("pct_policy_status_not_in_cancell"),
    )
    .select(
        "policy_id",
        F.coalesce(F.col("policy_status"), F.lit("ALL")).alias("policy_status"),
        "policy_holder_id",
        "gid",
        "policy_status_list",
        "stddev_claimed_amount_vs_reported_at_to_date_avg",
        "pct_policy_status_not_in_cancell",
    )
)
