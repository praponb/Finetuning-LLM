from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_month_claim_type = Window.partitionBy(F.month("reported_at"), "claim_type_or_unknown")
w_all_by_approved_amount_movin = Window.orderBy("approved_amount", "id").rowsBetween(-2, Window.currentRow)
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")

recent_claims = (
    df_claims
    .filter((F.col("status") == "PAID") & (F.col("claim_type") == "medical"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.date_format(F.col("reported_at"), "yyyy-MM-dd HH:00").alias("reported_at_label"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

with_trends = (
    recent_claims
    .select(
        "*",
        F.sum("approved_amount").over(w_reported_at_month_claim_type).alias("reported_at_month_claim_type_or_unknown_sum_appr"),
        F.round(F.avg("approved_amount").over(w_reported_at_month_claim_type), 2).alias("reported_at_month_claim_type_or_unknown_avg_appr"),
        F.round(F.avg("approved_amount").over(w_all_by_approved_amount_movin), 2).alias("approved_amount_avg_3"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_all_by_approved_amount))) / 60, 1).alias("minutes_since_prev_reported_at"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type").isin("fire", "theft"), 1)).alias("claim_claim_type_count"),
    )
)

with_stats = (
    with_trends.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    with_stats
    .cube("policy_id", "claim_claim_type_count", "adjuster_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.max("reported_at").alias("last_reported_at"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("claim_claim_type_count").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
