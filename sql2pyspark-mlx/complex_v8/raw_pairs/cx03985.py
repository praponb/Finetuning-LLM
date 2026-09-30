from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_or_unknown = Window.partitionBy("status_or_unknown")
w_status_len_by_claimed_amount = Window.partitionBy("status_len").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 5313.8) & (F.col("approved_amount") <= 32878.5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

sequenced = (
    base_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        "claim_type_prefix",
        "status_len",
        F.count(F.lit(1)).over(w_status_or_unknown).alias("rows_in_status_or_unknown"),
        F.last("claimed_amount", ignorenulls=True).over(w_status_len_by_claimed_amount).alias("last_known_claimed_amount"),
    )
)

summary = (
    sequenced
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("n_status_is_not_null"),
        F.round(F.avg("rows_in_status_or_unknown"), 2).alias("avg_rows_in_status_or_unknown"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.sum(F.when(~F.col("status").isin("APPROVED", "REJECTED"), F.col("status_len"))).alias("status_len_status_not_in_approved_rejec"),
        F.count(F.when(F.col("claim_type").isin("flood", "delay", "collision"), 1)).alias("n_claim_type_in_flood_delay_co"),
        F.max("rows_in_status_or_unknown").alias("max_rows_in_status_or_unknown_check"),
    )
    .filter(F.col("max_rows_in_status_or_unknown_check") > 9)
    .drop("max_rows_in_status_or_unknown_check")
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.sum("claimed_amount").alias("total_claim_claimed_amount"))
)

profiled = (
    summary.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.total_claim_claimed_amount")
)

result = (
    profiled
    .select(
        "adjuster_id",
        "claim_type_list",
        "status_len_status_not_in_approved_rejec",
        "total_claim_claimed_amount",
    )
    .orderBy(
        F.col("adjuster_id").desc(),
        F.col("total_claim_claimed_amount").asc_nulls_last(),
    )
    .limit(10)
)
